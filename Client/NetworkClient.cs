using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Shared.Network;

namespace Client;

public class NetworkClient : IDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly object _sync = new();
    private TcpClient? _tcp;
    private StreamReader? _reader;
    private StreamWriter? _writer;

    public NetworkClient(string host = "127.0.0.1", int port = 5005)
    {
        _host = host;
        _port = port;
    }

    public bool IsConnected => _tcp is { Connected: true };

    public string? ClientName { get; set; }

    public Response SendRequest(Request request) => SendTimed(request).Response;

    public TimedResponse SendTimed(Request request)
    {
        if (string.IsNullOrEmpty(request.ClientName) && !string.IsNullOrEmpty(ClientName))
            request = new Request { Operation = request.Operation, Payload = request.Payload, ClientName = ClientName };

        lock (_sync)
        {
            DateTime sentAt = DateTime.UtcNow;
            try
            {
                EnsureConnected();

                // Отсчёт RTT начинается после установки соединения, чтобы время
                // рукопожатия TCP не искажало измерение задержки.
                sentAt = DateTime.UtcNow;

                string jsonRequest = JsonSerializer.Serialize(request);
                _writer!.WriteLine(jsonRequest);

                string? jsonResponse = _reader!.ReadLine();
                DateTime receivedAt = DateTime.UtcNow;

                if (jsonResponse == null)
                {
                    Disconnect();
                    return new TimedResponse(
                        Response.Error("Error: server closed the connection (connection lost)."),
                        sentAt,
                        receivedAt);
                }

                return new TimedResponse(ReadResponse(jsonResponse), sentAt, receivedAt);
            }
            catch (SocketException)
            {
                Disconnect();
                return new TimedResponse(Response.Error("Error: server is unreachable (socket error)."), sentAt, DateTime.UtcNow);
            }
            catch (IOException)
            {
                Disconnect();
                return new TimedResponse(Response.Error("Error: server is unreachable (connection broken)."), sentAt, DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                Disconnect();
                return new TimedResponse(Response.Error($"Network error: {ex.Message}"), sentAt, DateTime.UtcNow);
            }
        }
    }

    private static Response ReadResponse(string jsonResponse)
    {
        try
        {
            Response? response = JsonSerializer.Deserialize<Response>(jsonResponse);
            if (response is null)
                return Response.Error("Error: empty response from server.");

            return response;
        }
        catch (JsonException ex)
        {
            // Ответ пришёл, но разбор не удался — это отдельный случай обработки ошибок.
            return Response.Error($"Error processing server response: {ex.Message}");
        }
    }

    private void EnsureConnected()
    {
        if (_tcp is { Connected: true } && _reader != null && _writer != null)
            return;

        Disconnect();

        _tcp = new TcpClient(_host, _port);
        var stream = _tcp.GetStream();
        _reader = new StreamReader(stream, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);
        _writer = new StreamWriter(stream, new UTF8Encoding(false), bufferSize: 4096, leaveOpen: true)
        {
            AutoFlush = true,
            NewLine = "\n"
        };
    }

    private void Disconnect()
    {
        try { _writer?.Dispose(); } catch { /* соединение уже разорвано */ }
        try { _reader?.Dispose(); } catch { /* соединение уже разорвано */ }
        try { _tcp?.Dispose(); } catch { /* соединение уже разорвано */ }
        _writer = null;
        _reader = null;
        _tcp = null;
    }

    public void Dispose() => Disconnect();
}

/// <summary>Ответ сервера плюс времена отправки и получения, измеренные часами клиента.</summary>
public readonly record struct TimedResponse(Response Response, DateTime SentAt, DateTime ReceivedAt);
