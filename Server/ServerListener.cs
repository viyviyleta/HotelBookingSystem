using System.Net;
using System.Net.Sockets;

namespace Server;

/// <summary>
/// Приём подключений и чтение запросов. Каждый клиент обслуживается в отдельной задаче,
/// поэтому запросы разных клиентов обрабатываются параллельно и попадают в одни и те же
/// критические секции бизнес-логики.
/// </summary>
public class ServerListener
{
    private readonly TcpListener _listener;
    private readonly RequestHandler _handler;
    private int _clientCounter = 0;

    public ServerListener(int port) : this(port, new RequestHandler())
    {
    }

    public ServerListener(int port, RequestHandler handler)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _handler = handler;
    }

    public void Start() => _listener.Start();

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Start();
        ServerLog.Info("SERVER", $"Running on port {ServerConfig.Port}, sync mode: {ServerConfig.SyncMode}, application mode: {ServerConfig.AppMode}");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(cancellationToken);
                ConnectionId clientId = new($"Client {Interlocked.Increment(ref _clientCounter)}");

                _ = Task.Run(() => HandleClientAsync(client, clientId), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            ServerLog.Info("SERVER", "Stopping: shutdown requested");
        }
        finally
        {
            _listener.Stop();
            ServerLog.Info("SERVER", "Stopped");
        }
    }

    public void Stop() => _listener.Stop();

    private async Task HandleClientAsync(TcpClient client, ConnectionId clientId)
    {
        ServerLog.Info(clientId.Name, $"CONNECTED (task {Task.CurrentId ?? -1}, thread {Environment.CurrentManagedThreadId})");

        try
        {
            using (client)
            using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream))
            using (var writer = new StreamWriter(stream) { AutoFlush = true })
            {
                // Клиент может прислать несколько запросов в одном соединении
                // (операции предметной области + TIME_REQUEST).
                while (true)
                {
                    string? jsonRequest = await reader.ReadLineAsync();
                    if (jsonRequest is null)
                        break;

                    if (string.IsNullOrWhiteSpace(jsonRequest))
                        continue;

                    string jsonResponse = await _handler.HandleRequestAsync(jsonRequest, clientId);
                    await writer.WriteLineAsync(jsonResponse);
                }
            }
        }
        catch (IOException ex)
        {
            // Обрыв соединения на стороне клиента — штатная ситуация для распределённого приложения.
            ServerLog.Info(clientId.Name, $"DISCONNECTED: {ex.Message}");
        }
        catch (SocketException ex)
        {
            ServerLog.Info(clientId.Name, $"DISCONNECTED: {ex.Message}");
        }
        catch (Exception ex)
        {
            ServerLog.Error(clientId.Name, $"{ex.GetType().Name}: {ex.Message}");
        }

        ServerLog.Info(clientId.Name, "DISCONNECTED");
    }
}
