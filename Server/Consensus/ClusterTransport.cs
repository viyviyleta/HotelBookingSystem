using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Shared.Network;

namespace Server.Consensus;

public sealed class ClusterNodeHost(ClusterNode node, ClusterJournal journal) : IDisposable
{
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;

    public ClusterNode Node { get; } = node;

    public int Port => Node.Port;

    public bool IsListening => _listener is not null;

    public int Start(int port)
    {
        Stop();

        _cts = new CancellationTokenSource();
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();

        int bound = ((IPEndPoint)_listener.LocalEndpoint).Port;
        Node.Bind(bound);

        TcpListener listener = _listener;
        CancellationToken token = _cts.Token;

        _ = Task.Run(() => AcceptLoopAsync(listener, token));

        journal.Node(Node.Id, $"{Node.Name} is listening on 127.0.0.1:{bound}");
        return bound;
    }

    public void Stop()
    {
        _cts?.Cancel();
        _cts = null;

        try
        {
            _listener?.Stop();
        }
        catch (SocketException)
        {
            // Слушатель уже остановлен - для отказа это штатная ситуация.
        }

        _listener = null;
    }

    public void Dispose() => Stop();

    private async Task AcceptLoopAsync(TcpListener listener, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(cancellationToken);
                _ = Task.Run(() => ServeAsync(client, cancellationToken), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Узел остановлен намеренно.
        }
        catch (ObjectDisposedException)
        {
            // Слушатель закрыли во время ожидания - это остановка узла.
        }
        catch (SocketException ex)
        {
            journal.Failure(Node.Id, $"{Node.Name} listener stopped: {ex.Message}");
        }
    }

    private async Task ServeAsync(TcpClient client, CancellationToken cancellationToken)
    {
        string remote = client.Client.RemoteEndPoint?.ToString() ?? "unknown";

        using (client)
        using (NetworkStream stream = client.GetStream())
        using (var reader = new StreamReader(stream, new UTF8Encoding(false), false, 4096, leaveOpen: true))
        using (var writer = new StreamWriter(stream, new UTF8Encoding(false), 4096, leaveOpen: true)
               { AutoFlush = true, NewLine = "\n" })
        {
            try
            {
                string? line = await reader.ReadLineAsync(cancellationToken);

                if (line is null)
                    return;

                Response? answer = Process(line, remote);

                if (answer is null)
                {
                    // Молчание: соединения не трогаем, пока отправитель сам не дождётся таймаута и не закроет его.
                    await reader.ReadLineAsync(CancellationToken.None);
                    return;
                }

                await writer.WriteLineAsync(JsonSerializer.Serialize(answer));
            }
            catch (IOException)
            {
                // Сосед оборвал соединение - для сети это обычное дело, узел продолжает работать.
            }
            catch (OperationCanceledException)
            {
                // Узел остановили, пока он обрабатывал сообщение.
            }
            catch (Exception ex)
            {
                journal.Error(Node.Id, $"{Node.Name} failed to serve {remote}: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    /// <summary>Разбор одного сообщения и ответ на него. null означает «узел молчит». Ошибочные сообщения узел не роняют.</summary>
    private Response? Process(string jsonLine, string remote)
    {
        Request? request;
        try
        {
            request = JsonSerializer.Deserialize<Request>(jsonLine);
        }
        catch (JsonException ex)
        {
            journal.Error(Node.Id, $"{Node.Name} received a malformed message from {remote}: {ex.Message}");
            return Response.Error($"Node {Node.Id} refused the message: it is not valid JSON");
        }

        if (request is null || string.IsNullOrWhiteSpace(request.Operation))
        {
            journal.Error(Node.Id, $"{Node.Name} received a message without a type from {remote}");
            return Response.Error($"Node {Node.Id} refused the message: the operation type is missing");
        }

        if (!MessageTypes.IsRequest(request.Operation))
        {
            journal.Error(Node.Id, $"{Node.Name} received a message it cannot handle ('{request.Operation}')");
            return Response.Error($"Node {Node.Id} refused the message: '{request.Operation}' is not a request type. " +
                                  $"Accepted requests: {string.Join(", ", MessageTypes.Requests)}");
        }

        ClusterMessage? message = ClusterMessage.FromRequest(request);

        if (message is null)
        {
            journal.Error(Node.Id, $"{Node.Name} received a body it cannot read as a cluster message");
            return Response.Error($"Node {Node.Id} refused the message: its body is not a cluster message");
        }

        if (message.To > 0 && message.To != Node.Id)
        {
            journal.Error(Node.Id, $"{Node.Name} got a message addressed to Node {message.To}");
            return Response.Error($"Node {Node.Id} refused the message: it is addressed to Node {message.To}");
        }

        try
        {
            ClusterMessage? reply = Node.Handle(message);

            return reply is null ? null : reply.ToResponse(true, reply.Details);
        }
        catch (NodeProcessingException ex)
        {
            return Response.Error(ex.Message);
        }
    }
}


public static class NodeChannel
{

    public static NodeReply Request(ClusterNode from, int toNodeId, int port, ClusterMessage message, int timeoutMs)
    {
        long began = Environment.TickCount64;
        DateTime startedAt = DateTime.UtcNow;
        string connection = $"tcp 127.0.0.1:{port}";

        using var client = new TcpClient();

        try
        {
            Connect(client, port, timeoutMs);
        }
        catch (SocketException ex)
        {
            from.CountTimeout();
            throw new NodeUnreachableException(toNodeId, $"connection failed: {ex.Message}");
        }
        catch (OperationCanceledException)
        {
            from.CountTimeout();
            throw new NodeTimeoutException(toNodeId, timeoutMs);
        }

        NetworkStream stream = client.GetStream();
        stream.ReadTimeout = timeoutMs;
        stream.WriteTimeout = timeoutMs;

        Response response;

        try
        {
            using var writer = new StreamWriter(stream, new UTF8Encoding(false), 4096, leaveOpen: true)
            { AutoFlush = true, NewLine = "\n" };

            writer.WriteLine(JsonSerializer.Serialize(message.ToRequest()));

            using var reader = new StreamReader(stream, new UTF8Encoding(false), false, 4096, leaveOpen: true);
            string? line = reader.ReadLine();

            if (line is null)
            {
                from.CountTimeout();
                throw new NodeUnreachableException(toNodeId, "the node closed the connection without an answer");
            }

            response = JsonSerializer.Deserialize<Response>(line) ?? new Response();
        }
        catch (IOException)
        {
            from.CountTimeout();
            throw new NodeTimeoutException(toNodeId, timeoutMs);
        }
        catch (SocketException)
        {
            // Таймаут чтения или обрыв соединения: узел принял сообщение, но не ответил вовремя.
            from.CountTimeout();
            throw new NodeTimeoutException(toNodeId, timeoutMs);
        }

        from.CountSent();

        int roundTripMs = (int)(Environment.TickCount64 - began);
        string answered = startedAt.ToString("HH:mm:ss.fff");

        if (!response.IsSuccess)
            return new NodeReply(toNodeId, $"Node {toNodeId}", message.Type, VoteResults.Refused, "FAILED", "",
                roundTripMs, response.Message, $"{connection}, refused at {answered}");

        ClusterMessage? reply = DeserializeReply(response);
        int id = reply is { From: > 0 } ? reply.From : toNodeId;

        return new NodeReply(
            id,
            $"Node {id}",
            reply?.Type ?? message.Type,
            VoteOf(reply, response),
            reply?.Status ?? "AVAILABLE",
            reply?.Role ?? "",
            roundTripMs,
            reply?.Details ?? (response.Message.Length > 0 ? response.Message : $"{reply?.Type ?? "ANSWERED"}"),
            $"{connection}, answered at {answered}",
            reply?.KnownPeers ?? 0);
    }

    private static string VoteOf(ClusterMessage? reply, Response response) => reply switch
    {
        null => "ANSWERED",
        { Vote.Length: > 0 } => reply.Vote,
        _ when reply.Status == "FAILED" => "FAILED",
        _ when reply.Type == MessageTypes.Alive => MessageTypes.Alive,
        _ when reply.Status.Length > 0 => reply.Status,
        _ when reply.Type.Length > 0 => reply.Type,
        _ => "ANSWERED"
    };

    private static void Connect(TcpClient client, int port, int timeoutMs)
    {
        using var timeout = new CancellationTokenSource(timeoutMs);

        client.ConnectAsync(IPAddress.Loopback, port, timeout.Token).GetAwaiter().GetResult();
    }

    private static ClusterMessage? DeserializeReply(Response response)
    {
        if (string.IsNullOrEmpty(response.Data))
            return null;

        try
        {
            return JsonSerializer.Deserialize<ClusterMessage>(response.Data);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
