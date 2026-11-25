using Shared.Network;

namespace Client;


public sealed class TimeSyncService
{
    private readonly NetworkClient _client;

    public ClientClock Clock { get; }

    public TimeSyncService(NetworkClient client, ClientClock clock)
    {
        _client = client;
        Clock = clock;
    }

    public bool TrySynchronize(int serverDelayMs, out TimeExchange? exchange, out string error)
    {
        exchange = null;
        error = string.Empty;

        TimedResponse timed = _client.SendTimed(RequestBuilder.BuildGetServerTime(serverDelayMs));

        if (!timed.Response.IsSuccess)
        {
            error = timed.Response.Message;
            return false;
        }

        DateTime serverAnswered = timed.Response.ServerTime;
        DateTime serverReceived = timed.Response.ServerReceivedTime;

        if (serverAnswered == default || serverReceived == default)
        {
            error = "Invalid time data: server response has no time stamps";
            return false;
        }

        exchange = new TimeExchange(timed.SentAt, timed.ReceivedAt, serverReceived, serverAnswered, Clock);
        return true;
    }

    /// Эталонное время сервера для оценки погрешности после синхронизации
    /// (с поправкой на половину RTT самого проверочного запроса).
    public bool TryGetServerNow(out DateTime serverNow, out string error)
    {
        serverNow = default;
        error = string.Empty;

        if (!TrySynchronize(0, out TimeExchange? exchange, out error))
            return false;

        serverNow = exchange!.ServerAnsweredAt + exchange.EstimatedOneWayDelay;
        return true;
    }
}
