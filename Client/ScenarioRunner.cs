using Shared.Network;

namespace Client;

public sealed partial class ScenarioRunner
{
    private readonly int _port;
    private readonly string _host;
    private readonly ClientClock _clock;
    private readonly NetworkClient _client;
    private readonly TimeSyncService _time;

    public ScenarioRunner(NetworkClient client, ClientClock clock, int port, string host = "127.0.0.1")
    {
        _client = client;
        _clock = clock;
        _port = port;
        _host = host;
        _time = new TimeSyncService(client, clock);
    }

    public void PrintLocalTime()
    {
        ClientLog.Info($"{_clock} → local time: {_clock.LocalNow():HH:mm:ss.fff} (raw UTC: {DateTime.UtcNow:HH:mm:ss.fff})");
    }

    public bool ServerAvailable()
    {
        Response response = _client.SendRequest(RequestBuilder.BuildGetServerTime(0));
        return response.IsSuccess;
    }

    public bool RunTimeExperiments(int[] serverDelaysMs)
    {
        ClientLog.Line("");
        ClientLog.Line("=== PART 2: CLOCK SYNCHRONIZATION ===");
        ClientLog.Info($"Clock model: {_clock}, skew = {_clock.Skew.TotalSeconds.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture)} s");

        foreach (int delay in serverDelaysMs)
        {
            if (!TrySynchronizeOnce(delay, out TimeExchange? exchange, out string error))
                continue;

            PrintExchange(exchange!, delay);
        }

        return true;
    }

    private bool TrySynchronizeOnce(int delay, out TimeExchange? exchange, out string error)
    {
        ClientLog.Line($"\n--- TIME_REQUEST (server artificial delay = {delay} ms) ---");

        if (!_time.TrySynchronize(delay, out exchange, out error))
        {
            ClientLog.Info($"FAIL: {error}");
            exchange = null;
            return false;
        }

        return true;
    }

    private void PrintExchange(TimeExchange exchange, int delay)
    {
        ClientLog.Line($"Request time  (client raw clock): {T(exchange.ClientSendRaw)}");
        ClientLog.Line($"Response time (client raw clock): {T(exchange.ClientReceiveRaw)}");
        ClientLog.Line($"RTT = {S(exchange.Rtt.TotalSeconds)} s, estimated one-way delay RTT/2 = {S(exchange.EstimatedOneWayDelay.TotalSeconds)} s");
        ClientLog.Line($"Server received request at: {T(exchange.ServerReceivedAt)}, server answered at: {T(exchange.ServerAnsweredAt)} (artificial delay {delay} ms)");
        ClientLog.Line($"Client local time:  {T(exchange.LocalTimeAtSend)}");
        ClientLog.Line($"Server time:        {T(exchange.ServerAnsweredAt)}");
        ClientLog.Line($"Difference before synchronization: {S(exchange.DifferenceBeforeSync.TotalSeconds)} s");

        if (!_time.TryGetServerNow(out DateTime serverNow, out string error))
        {
            ClientLog.Info($"FAIL (reference server time): {error}");
            return;
        }

        ClientLog.Line($"Reference server time now: {T(serverNow)}");

        DateTime rawNow = DateTime.UtcNow;

        foreach (CorrectionStrategy strategy in Enum.GetValues<CorrectionStrategy>())
        {
            TimeSpan correction = exchange.Correction(strategy);
            DateTime corrected = exchange.CorrectedAt(rawNow, strategy);
            TimeSpan residual = exchange.Error(strategy, rawNow, serverNow);

            ClientLog.Line(
                $"{StrategyName(strategy),-18} offset = {S(correction.TotalSeconds)} s → " +
                $"corrected time = {T(corrected)}, difference after synchronization = {S(residual.TotalSeconds)} s");
        }
    }

    private static string StrategyName(CorrectionStrategy strategy) => strategy switch
    {
        CorrectionStrategy.Naive => "naive (Req.4)",
        CorrectionStrategy.HalfRtt => "RTT/2 (Req.5-6)",
        CorrectionStrategy.Averaged => "averaged (Req.7)",
        _ => strategy.ToString()
    };

    public void RunRace(int roomId, int clientsCount, int checkInOffsetDays = 1)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== PART 1: CONCURRENT RESERVE, room {roomId}, clients = {clientsCount} ===");

        List<(string Who, TimedResponse Timed)> results = ConcurrentReserve(roomId, clientsCount,
            number => RequestBuilder.BuildCreateBooking(
                roomId,
                DateTime.Today.AddDays(checkInOffsetDays),
                DateTime.Today.AddDays(checkInOffsetDays + 2),
                guestId: 900 + number));

        foreach ((string who, TimedResponse timed) in results)
            ClientLog.Line(
                $"{who} → request {T(timed.SentAt)}, response {T(timed.ReceivedAt)} → " +
                $"{(timed.Response.IsSuccess ? "SUCCESS" : "FAIL")}: {timed.Response.Message}");

        ClientLog.Line("=== room state after race ===");
        Response state = _client.SendRequest(RequestBuilder.BuildCheckAvailability());
        ClientLog.Line($"CheckAvailability: {state.Message} → {state.Data}");
    }

    /// <summary>
    /// N отдельных соединений, стартующих по одному барьеру: запросы гарантированно
    /// пересекаются на сервере. Возвращает ответы в порядке получения, а не запуска.
    /// </summary>
    private List<(string Who, TimedResponse Timed)> ConcurrentReserve(int roomId, int clientsCount,
        Func<int, Request> requestFor)
    {
        using var gate = new ManualResetEventSlim(false);
        var results = new System.Collections.Concurrent.ConcurrentQueue<(string, TimedResponse)>();

        Task[] workers = Enumerable.Range(1, clientsCount).Select(number => Task.Run(() =>
        {
            using var raceClient = new NetworkClient(_host, _port)
            {
                ClientName = $"{ClientLog.Who} #{number}"
            };

            // Прогрев соединения, чтобы время установки TCP не попадало в измерение.
            raceClient.SendRequest(RequestBuilder.BuildGetServerTime(0));

            Request request = requestFor(number);

            gate.Wait();
            TimedResponse timed = raceClient.SendTimed(request);

            results.Enqueue(($"{ClientLog.Who} #{number}", timed));
        })).ToArray();

        gate.Set();
        Task.WhenAll(workers).GetAwaiter().GetResult();

        return results.OrderBy(entry => entry.Item1).ToList();
    }


    public void ReserveOnce(int roomId)
    {
        Request request = RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1);

        ClientLog.Line($"[{_clock}] local time {T(_clock.LocalNow())} (raw UTC {T(DateTime.UtcNow)}) → CreateBooking room {roomId}");

        TimedResponse timed = _client.SendTimed(request);

        ClientLog.Line($"[{_clock}] local time {T(_clock.LocalNow())} (raw UTC {T(timed.ReceivedAt)}) → " +
                       $"{(timed.Response.IsSuccess ? "SUCCESS" : "FAIL")}: {timed.Response.Message}");
    }
    
    public void RunErrorChecks()
    {
        ClientLog.Line("");
        ClientLog.Line("=== ERROR HANDLING CHECKS ===");

        using var closedClient = new NetworkClient("127.0.0.1", 5999);
        ClientLog.Line($"Unavailable server        → {closedClient.SendRequest(RequestBuilder.BuildCheckAvailability()).Message}");

        ClientLog.Line($"Unknown operation         → {_client.SendRequest(new Request { Operation = "TeleportRoom", Payload = "{}" }).Message}");
        ClientLog.Line($"Malformed payload         → {_client.SendRequest(RequestBuilder.BuildInvalidRequest()).Message}");
        ClientLog.Line($"Missing room              → {_client.SendRequest(RequestBuilder.BuildCreateBooking(999, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 1)).Message}");

        // Номер 102 занимается заранее, чтобы вторая попытка упёрлась в занятое состояние.
        _client.SendRequest(RequestBuilder.BuildCreateBooking(102, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 2));
        ClientLog.Line($"Already reserved room     → {_client.SendRequest(RequestBuilder.BuildCreateBooking(102, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 3)).Message}");
        ClientLog.Line($"Non-existent booking      → {_client.SendRequest(new Request { Operation = "PayBooking", Payload = """{"BookingId":4242}""" }).Message}");
        ClientLog.Line($"Invalid time data         → {_client.SendRequest(new Request { Operation = "GetServerTime", Payload = """{"DelayMs": -5}""" }).Message}");
        ClientLog.Line($"Time data not a number    → {_client.SendRequest(new Request { Operation = "GetServerTime", Payload = """{"DelayMs": "abc"}""" }).Message}");
    }

    private static string T(DateTime value) => value.ToString("HH:mm:ss.fff");

    /// <summary>Секунды с обязательным знаком и точностью до миллисекунд (разделитель — точка).</summary>
    private static string S(double seconds)
    {
        double normalized = Math.Abs(seconds) < 0.0005 ? 0 : seconds;
        return normalized.ToString("+0.000;-0.000", System.Globalization.CultureInfo.InvariantCulture);
    }
}
