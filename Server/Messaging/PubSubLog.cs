namespace Server.Messaging;

/// <summary>
/// Строки журнала Pub/Sub в формате методички («Server → Event», «Publisher → PUBLISH»,
/// «Broker → EVENT RECEIVED», «Broker → Subscriber», «Subscriber → SUCCESS»): метка клиента
/// остаётся той же, что и у строк операции, а роль компонента стоит в квадратных скобках.
/// </summary>
public static class PubSubLog
{
    public static void Formed(string clientId, DomainEvent domainEvent) =>
        ServerLog.Info(clientId, $"→ [EVENT] Server → {domainEvent.EventType} ({domainEvent.DataText})");

    public static void Publish(string clientId, DomainEvent domainEvent, string broker) =>
        ServerLog.Info(clientId, $"→ [PUBLISHER] PUBLISH {domainEvent.EventType} #{domainEvent.EventId} → Broker {broker}");

    public static void Received(string clientId, string broker, DomainEvent domainEvent, int recipients) =>
        ServerLog.Info(clientId, $"→ [BROKER {broker}] EVENT RECEIVED {domainEvent.EventType} #{domainEvent.EventId}, " +
                                 $"subscribers = {recipients}");

    public static void Sending(string clientId, string broker, string subscriber) =>
        ServerLog.Info(clientId, $"→ [BROKER {broker}] → {subscriber}");

    public static void Handled(string clientId, string subscriber, string outcome, string detail) =>
        ServerLog.Info(clientId, $"→ [{subscriber}] {outcome} {detail}");

    public static void BrokerState(string clientId, string broker, string state, string detail) =>
        ServerLog.Info(clientId, $"→ [BROKER {broker}] {state} {detail}");

    public static void PublisherFailed(string clientId, string broker, string error) =>
        ServerLog.Info(clientId, $"→ [PUBLISHER] FAIL - Broker {broker} refused the event: {error}");

    /// <summary>Итог публикации относится к Publisher - общий [RESULT] операции остаётся единственным.</summary>
    public static void PublisherResult(string clientId, string detail) =>
        ServerLog.Info(clientId, $"→ [PUBLISHER] {detail}");

    public static void Subscriber(string clientId, string subscriber, string detail) =>
        ServerLog.Info(clientId, $"→ [SUBSCRIBER] {subscriber} → {detail}");

    public static void Subscribe(string clientId, string subscriber, string broker, string eventTypes) =>
        ServerLog.Info(clientId, $"→ [SUBSCRIBE] {subscriber} → Subscribe({eventTypes}) on Broker {broker}");

    public static void Queue(string clientId, string broker, string detail) =>
        ServerLog.Info(clientId, $"→ [QUEUE {broker}] {detail}");

    public static void Retry(string clientId, string broker, string detail) =>
        ServerLog.Info(clientId, $"→ [RETRY {broker}] {detail}");

    public static void Statistics(string clientId, string detail) =>
        ServerLog.Info(clientId, $"→ [STATS] {detail}");
}
