namespace Client;


/// Клиентские копии отчётов сервера о механизме Pub/Sub. Клиент не ссылается на типы сервера:
/// он читает тот же JSON, который сервер положил в поле Data ответа (как в ЛР №4 и ЛР №5).

public sealed record SubscriberStatus(
    string Name,
    string Kind,
    string Broker,
    string State,
    string Reason,
    int DelayMs,
    List<string> Subscriptions,
    int Handled,
    int Failed,
    int Pending,
    List<string> LastResults);

public sealed record BrokerStatus(
    string Name,
    string State,
    string Reason,
    int Accepted,
    int Attempts,
    int Pending,
    List<string> Subscribers,
    List<string> PendingEvents);

public sealed record PubSubStatusReport(
    bool Enabled,
    string Summary,
    List<BrokerStatus> Brokers,
    List<SubscriberStatus> Subscribers,
    int PublishedEvents,
    int FailedPublications,
    int MaxDeliveryAttempts);

public sealed record SubscriberTiming(
    string Subscriber,
    string Broker,
    string Outcome,
    int Attempts,
    int ProcessingMs,
    string StartedAt,
    string FinishedAt,
    string? Error,
    string Result);

public sealed record EventTiming(
    string EventId,
    string EventType,
    int EntityId,
    string ClientId,
    string PublishedAt,
    int PublishMs,
    int FirstDeliveryMs,
    int MaxProcessingMs,
    int AllDoneMs,
    string Publication,
    List<SubscriberTiming> Subscribers);

public sealed record EventStatisticsReport(
    int TotalEvents,
    int TotalDeliveries,
    int TotalFailures,
    string Summary,
    List<EventTiming> Events);
