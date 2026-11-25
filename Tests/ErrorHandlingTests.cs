using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Client;
using Server;
using Shared.Models;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Требование 11 (Часть 2): обработка ошибок на транспортном уровне и на уровне протокола.
/// </summary>
public class ErrorHandlingTests
{
    private static int _port = 5400;

    private static int NextPort() => Interlocked.Increment(ref _port);

    private static async Task<Response> SendAsync(RequestHandler handler, string rawRequest)
    {
        string json = await handler.HandleRequestAsync(rawRequest, "TestClient");
        return JsonSerializer.Deserialize<Response>(json)!;
    }

    [Fact]
    public async Task UnknownOperation_IsReported()
    {
        var handler = new RequestHandler();

        Response response = await SendAsync(handler,
            JsonSerializer.Serialize(new Request { Operation = "TeleportGuest", Payload = "{}" }));

        Assert.False(response.IsSuccess);
        Assert.Contains("Unknown operation", response.Message);
    }

    [Fact]
    public async Task MalformedRequestJson_IsReported()
    {
        var handler = new RequestHandler();

        Response broken = await SendAsync(handler, "{ this is not json }");
        Response empty = await SendAsync(handler, "");
        Response noOperation = await SendAsync(handler,
            JsonSerializer.Serialize(new Request { Operation = "", Payload = "{}" }));

        Assert.False(broken.IsSuccess);
        Assert.Contains("Invalid request format", broken.Message);
        Assert.False(empty.IsSuccess);
        Assert.False(noOperation.IsSuccess);
    }

    [Fact]
    public async Task MissingRoomAndDoubleBooking_AreReported()
    {
        DataStorage.Reset();
        ServerConfig.ReserveProcessingDelayMs = 5;
        var handler = new RequestHandler(new LockBusinessLogic());

        string payload = JsonSerializer.Serialize(new Booking
        {
            RoomId = 101,
            GuestId = 1,
            CheckInDate = DateTime.Today.AddDays(1),
            CheckOutDate = DateTime.Today.AddDays(2)
        });
        string request = JsonSerializer.Serialize(new Request { Operation = "CreateBooking", Payload = payload });

        string missingPayload = request.Replace("101", 777.ToString());

        Response missing = await SendAsync(handler, missingPayload);
        Response first = await SendAsync(handler, request);
        Response second = await SendAsync(handler, request);

        Assert.False(missing.IsSuccess);
        Assert.Contains("does not exist", missing.Message);
        Assert.True(first.IsSuccess);
        Assert.False(second.IsSuccess);
        Assert.Contains("already reserved", second.Message);
    }

    [Fact]
    public void Client_ReportsUnavailableServer()
    {
        using var client = new NetworkClient("127.0.0.1", 5999);

        Response response = client.SendRequest(RequestBuilder.BuildCheckAvailability());

        Assert.False(response.IsSuccess);
        Assert.Contains("unreachable", response.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Client_ReportsClosedConnection()
    {
        int port = NextPort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();

        // Сервер принимает соединение и сразу его закрывает — клиент обязан это заметить.
        _ = Task.Run(() =>
        {
            using TcpClient accepted = listener.AcceptTcpClient();
            accepted.Close();
        });

        using var client = new NetworkClient("127.0.0.1", port);
        Response response = client.SendRequest(RequestBuilder.BuildCheckAvailability());

        listener.Stop();

        Assert.False(response.IsSuccess);
        Assert.Contains("connection", response.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Client_ReportsCorruptedResponse()
    {
        int port = NextPort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();

        _ = Task.Run(() =>
        {
            using TcpClient accepted = listener.AcceptTcpClient();
            using var writer = new StreamWriter(accepted.GetStream(), new UTF8Encoding(false))
            {
                AutoFlush = true,
                NewLine = "\n"
            };
            writer.WriteLine("<<not json>>");
        });

        using var client = new NetworkClient("127.0.0.1", port);
        Response response = client.SendRequest(RequestBuilder.BuildCheckAvailability());

        listener.Stop();

        Assert.False(response.IsSuccess);
        Assert.Contains("response", response.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TwoClients_OverTcp_ReserveSameRoom_OnlyOneSucceeds()
    {
        DataStorage.Reset();
        ServerConfig.ReserveProcessingDelayMs = 400;
        ServerConfig.Port = NextPort();

        var listener = new ServerListener(ServerConfig.Port);
        using var cts = new CancellationTokenSource();
        Task running = listener.StartAsync(cts.Token);

        try
        {
            string payload = JsonSerializer.Serialize(new Booking
            {
                RoomId = 101,
                GuestId = 1,
                CheckInDate = DateTime.Today.AddDays(1),
                CheckOutDate = DateTime.Today.AddDays(3)
            });
            var request = new Request { Operation = "CreateBooking", Payload = payload };

            Task<Response>[] reservations =
            [
                Task.Run(() => { using var c = new NetworkClient("127.0.0.1", ServerConfig.Port); return c.SendRequest(request); }),
                Task.Run(() => { using var c = new NetworkClient("127.0.0.1", ServerConfig.Port); return c.SendRequest(request); })
            ];

            Response[] responses = await Task.WhenAll(reservations);

            Assert.Equal(1, responses.Count(r => r.IsSuccess));
            Assert.Single(DataStorage.Bookings);
        }
        finally
        {
            cts.Cancel();
            listener.Stop();
            await running;
            ServerConfig.ReserveProcessingDelayMs = 1500;
        }
    }

    [Fact]
    public async Task Client_SendsTimeRequest_AndGetsServerStamps()
    {
        ServerConfig.Port = NextPort();
        var listener = new ServerListener(ServerConfig.Port);
        using var cts = new CancellationTokenSource();
        Task running = listener.StartAsync(cts.Token);

        try
        {
            using var client = new NetworkClient("127.0.0.1", ServerConfig.Port);

            // Один клиент может задать задержку ответа сервера (Требование 7, Часть 2).
            Response response = client.SendRequest(RequestBuilder.BuildGetServerTime(200));

            Assert.True(response.IsSuccess);
            Assert.True(response.ServerTime - response.ServerReceivedTime >= TimeSpan.FromMilliseconds(150));
        }
        finally
        {
            cts.Cancel();
            listener.Stop();
            await running;
        }
    }

    [Fact]
    public void ClientClock_SkewIsAppliedToLogs()
    {
        var clock = new ClientClock(TimeSpan.FromSeconds(7), () => new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc));

        Assert.Equal(new DateTime(2026, 1, 1, 12, 0, 7, DateTimeKind.Utc), clock.LocalNow());
    }
}
