namespace Server.Messaging;

// ------------------------------------------------------------- ЗАПРОСЫ КЛИЕНТА

/// <summary>Ручная публикация события (эксперименты Части 1 и проверка ошибок Части 5 требования 5).</summary>
public sealed class PublishCommand
{
    public string EventType { get; set; } = string.Empty;

    public int EntityId { get; set; }

    public string? RoomId { get; set; }

    public Dictionary<string, string> Data { get; set; } = new();
}

public sealed class AddSubscriberCommand
{
    public string Name { get; set; } = string.Empty;

    /// <summary>Тип подписчика: logger | room | notification | audit | analytics.</summary>
    public string Kind { get; set; } = "audit";

    public string Broker { get; set; } = string.Empty;

    public List<string> EventTypes { get; set; } = new();
}

public sealed class SubscribeCommand
{
    public string Subscriber { get; set; } = string.Empty;

    public string EventType { get; set; } = string.Empty;
}

public sealed class UnsubscribeCommand
{
    public string Subscriber { get; set; } = string.Empty;

    public string EventType { get; set; } = string.Empty;
}

/// <summary>
/// Отказ подписчика. Mode выбирает способ из списка методички: смена состояния, ошибка соединения
/// или ошибка обработки; Attempts - сколько попыток подряд завершится неудачей (для повторной доставки).
/// </summary>
public sealed class FailSubscriberCommand
{
    public string Name { get; set; } = string.Empty;

    public string Mode { get; set; } = "unavailable";

    public int Attempts { get; set; } = 1;

    public string Reason { get; set; } = string.Empty;
}

public sealed class SubscriberNameCommand
{
    public string Name { get; set; } = string.Empty;
}

public sealed class BrokerNameCommand
{
    public string Name { get; set; } = string.Empty;

    public string Reason { get; set; } = string.Empty;
}

// -------------------------------------------------------------------- ОТЧЁТЫ

public sealed record SubscriberStatus(
    string Name,
    string Kind,
    string Broker,
    string State,
    string Reason,
    int DelayMs,
    IReadOnlyList<string> Subscriptions,
    int Handled,
    int Failed,
    int Pending,
    IReadOnlyList<string> LastResults);

public sealed record BrokerStatus(
    string Name,
    string State,
    string Reason,
    int Accepted,
    int Attempts,
    int Pending,
    IReadOnlyList<string> Subscribers,
    IReadOnlyList<string> PendingEvents);

public sealed record PubSubStatusReport(
    bool Enabled,
    string Summary,
    IReadOnlyList<BrokerStatus> Brokers,
    IReadOnlyList<SubscriberStatus> Subscribers,
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
    IReadOnlyList<SubscriberTiming> Subscribers);

public sealed record EventStatisticsReport(
    int TotalEvents,
    int TotalDeliveries,
    int TotalFailures,
    string Summary,
    IReadOnlyList<EventTiming> Events);
