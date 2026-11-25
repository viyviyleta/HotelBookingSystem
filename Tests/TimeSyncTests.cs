using System.Text.Json;
using Client;
using Server;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 2: расчёт поправки часов клиента (Требования 4–7) и операции получения времени сервера.
/// </summary>
public class TimeSyncTests
{
    private static readonly DateTime BaseUtc = new(2026, 9, 23, 10, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Skew = TimeSpan.FromSeconds(7);

    /// <summary>
    /// Синтезирует один обмен с известными задержками: f — туда, b — обратно, d — сервер «думает» перед ответом.
    /// </summary>
    private static TimeExchange Exchange(
        TimeSpan forward,
        TimeSpan back,
        TimeSpan serverProcessing,
        TimeSpan skew = default)
    {
        DateTime sendRaw = BaseUtc;
        DateTime serverReceived = sendRaw + forward;
        DateTime serverAnswered = serverReceived + serverProcessing;
        DateTime receiveRaw = serverAnswered + back;

        return new TimeExchange(sendRaw, receiveRaw, serverReceived, serverAnswered, new ClientClock(skew));
    }

    private static TimeSpan ErrorVsTrueServerTime(TimeExchange exchange, CorrectionStrategy strategy)
    {
        // Эталон: «истинное» время сервера в момент приёма ответа. Часы сервера в модели совпадают с UTC,
        // поэтому эталоном служит сырое время клиента в тот же момент.
        DateTime trueServerTime = exchange.ClientReceiveRaw;
        return exchange.Error(strategy, exchange.ClientReceiveRaw, trueServerTime);
    }

    [Fact]
    public void ClientClock_AddsModeledSkew()
    {
        var clock = new ClientClock(Skew);

        DateTime local = clock.LocalNow();
        DateTime raw = DateTime.UtcNow;

        Assert.InRange(local - raw, TimeSpan.FromSeconds(6.9), TimeSpan.FromSeconds(7.1));
    }

    [Fact]
    public void LocalTime_DiffersFromServerTime_ByTheSkew()
    {
        TimeExchange exchange = Exchange(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, Skew);

        Assert.Equal(7, exchange.DifferenceBeforeSync.TotalSeconds, 0);
    }

    [Fact]
    public void Rtt_IsRoundTripTimeMeasuredByClient()
    {
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2));

        Assert.Equal(4, exchange.Rtt.TotalSeconds, 0);
        Assert.Equal(2, exchange.EstimatedOneWayDelay.TotalSeconds, 0);
    }

    [Fact]
    public void NaiveCorrection_IgnoresDelay_AndShiftsTimeByTheWholeForwardPath()
    {
        // Требование 4: Offset = ServerTime - ClientTime.
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2),
            Skew);

        // Ошибка такой поправки равна f + d = 3 с (задержка не учтена).
        Assert.Equal(3, ErrorVsTrueServerTime(exchange, CorrectionStrategy.Naive).TotalSeconds, 0);
    }

    [Fact]
    public void HalfRttCorrection_CompensatesSymmetricNetworkDelay()
    {
        // Требования 5–6: RTT/2 вычитается из оценки серверного времени.
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.Zero,
            Skew);

        Assert.Equal(0, ErrorVsTrueServerTime(exchange, CorrectionStrategy.HalfRtt).TotalSeconds, 0);
        Assert.Equal(-7, exchange.Correction(CorrectionStrategy.HalfRtt).TotalSeconds, 0);
    }

    [Fact]
    public void HalfRttCorrection_IsBiasedByServerProcessingDelay()
    {
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2),
            Skew);

        // Остаточная ошибка = (f + d - b) / 2 = 1 с: задержка обработки на сервере снижает точность.
        Assert.Equal(1, ErrorVsTrueServerTime(exchange, CorrectionStrategy.HalfRtt).TotalSeconds, 0);
    }

    [Fact]
    public void AveragedCorrection_IsNotAffectedByServerProcessingDelay()
    {
        TimeExchange withoutDelay = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(0),
            Skew);

        TimeExchange withDelay = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5),
            Skew);

        Assert.Equal(
            withoutDelay.Correction(CorrectionStrategy.Averaged).TotalSeconds,
            withDelay.Correction(CorrectionStrategy.Averaged).TotalSeconds,
            3);

        Assert.Equal(0, ErrorVsTrueServerTime(withDelay, CorrectionStrategy.Averaged).TotalSeconds, 0);
    }

    [Fact]
    public void Corrections_AreOrdered_ByAccuracy()
    {
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2),
            Skew);

        double naive = Math.Abs(ErrorVsTrueServerTime(exchange, CorrectionStrategy.Naive).TotalSeconds);
        double halfRtt = Math.Abs(ErrorVsTrueServerTime(exchange, CorrectionStrategy.HalfRtt).TotalSeconds);
        double averaged = Math.Abs(ErrorVsTrueServerTime(exchange, CorrectionStrategy.Averaged).TotalSeconds);

        Assert.True(naive > halfRtt);
        Assert.True(halfRtt >= averaged);
    }

    [Fact]
    public void AsymmetricDelay_LeavesResidualError_EvenAfterSynchronization()
    {
        TimeExchange exchange = Exchange(
            TimeSpan.FromSeconds(3),
            TimeSpan.FromSeconds(0.2),
            TimeSpan.Zero,
            Skew);

        Assert.True(Math.Abs(ErrorVsTrueServerTime(exchange, CorrectionStrategy.HalfRtt).TotalSeconds) > 1);
    }

    private static async Task<Response> GetTimeAsync(string payload)
    {
        var handler = new RequestHandler();
        string json = await handler.HandleRequestAsync(
            JsonSerializer.Serialize(new Request { Operation = "GetServerTime", Payload = payload }),
            "TestClient");

        return JsonSerializer.Deserialize<Response>(json)!;
    }

    [Fact]
    public async Task ServerTimeOperation_ReturnsBothStamps()
    {
        Response response = await GetTimeAsync("{}");

        Assert.True(response.IsSuccess);
        Assert.True(response.ServerTime > DateTime.MinValue);
        Assert.True(response.ServerReceivedTime <= response.ServerTime);
    }

    [Fact]
    public async Task ServerTimeOperation_RespectsArtificialDelay()
    {
        var before = DateTime.UtcNow;
        Response response = await GetTimeAsync("""{"DelayMs": 400}""");
        var after = DateTime.UtcNow;

        Assert.True(response.IsSuccess);
        Assert.True(after - before >= TimeSpan.FromMilliseconds(380));
        Assert.True(response.ServerTime - response.ServerReceivedTime >= TimeSpan.FromMilliseconds(380));
    }

    [Fact]
    public async Task ServerTimeOperation_RejectsInvalidTimeData()
    {
        foreach (string payload in new[] { """{"DelayMs": -5}""", """{"DelayMs": "abc"}""", """{"DelayMs": 999999}""", "not json"})
        {
            Response response = await GetTimeAsync(payload);
            Assert.False(response.IsSuccess);
            Assert.Contains("Invalid", response.Message);
        }
    }

    [Fact]
    public async Task ClientSynchronizesClockWithRunningServer()
    {
        int port = 5391;
        ServerConfig.Port = port;
        ServerConfig.DefaultTimeResponseDelayMs = 0;

        var listener = new ServerListener(port);
        using var cts = new CancellationTokenSource();
        Task running = listener.StartAsync(cts.Token);

        try
        {
            using var client = new NetworkClient("127.0.0.1", port);
            var clock = new ClientClock(Skew);
            var sync = new TimeSyncService(client, clock);

            Assert.True(sync.TrySynchronize(serverDelayMs: 0, out TimeExchange? exchange, out string error), error);

            // До синхронизации клиент «впереди» сервера примерно на величину смещения.
            Assert.True(exchange!.DifferenceBeforeSync > TimeSpan.FromSeconds(6));

            DateTime corrected = exchange.CorrectedNow(CorrectionStrategy.HalfRtt);
            Assert.True(corrected - DateTime.UtcNow < TimeSpan.FromMilliseconds(500));

            // После синхронизации скорректированное время клиента совпадает с серверным с точностью задержки.
            Assert.True(sync.TryGetServerNow(out DateTime serverNow, out error), error);
            Assert.True(Math.Abs((corrected - serverNow).TotalMilliseconds) < 500);
        }
        finally
        {
            cts.Cancel();
            listener.Stop();
            await running;
        }
    }
}
