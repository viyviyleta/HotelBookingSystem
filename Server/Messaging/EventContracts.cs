using System.Text.Json.Serialization;

namespace Server.Messaging;

/// <summary>
/// Тип события = идентификатор подписки (Часть 1, Требование 2). Для варианта 2 это события
/// бронирования отелей: создание брони, отмена брони, завершение оплаты.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EventType
{
    BookingCreated,
    BookingCancelled,
    PaymentCompleted
}

public static class EventTypes
{
    public static readonly IReadOnlyList<string> Names = Enum.GetNames<EventType>();

    /// <summary>Разбор имени события из запроса клиента: неверное имя - отказ, а не исключение.</summary>
    public static bool TryParse(string? value, out EventType type)
    {
        type = default;

        if (string.IsNullOrWhiteSpace(value))
            return false;

        foreach (EventType candidate in Enum.GetValues<EventType>())
        {
            if (string.Equals(candidate.ToString(), value.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                type = candidate;
                return true;
            }
        }

        return false;
    }
}

/// <summary>Событие предметной области: тип + идентификатор объекта + время + данные для подписчиков.</summary>
public sealed record DomainEvent(
    string EventId,
    EventType EventType,
    int EntityId,
    DateTime Timestamp,
    string ClientId,
    IReadOnlyDictionary<string, string> Data)
{
    public string DataText => Data.Count == 0
        ? "-"
        : string.Join(", ", Data.Select(entry => $"{entry.Key} = {entry.Value}"));

    public string ShortText => $"{EventType} #{EventId} (entity = {EntityId}, client = {ClientId})";

    public string FullText => $"{ShortText}, time = {Timestamp:HH:mm:ss.fff}, data = {DataText}";
}

/// <summary>События создаёт сервер, но проверяет их Publisher: некорректное событие не должно доходить до Broker.</summary>
public static class DomainEventFactory
{
    private static long _sequence;

    public static DomainEvent Create(EventType type, int entityId, string clientId,
        IDictionary<string, string>? data = null) =>
        new(NextEventId(), type, entityId, DateTime.UtcNow, clientId,
            (data ?? new Dictionary<string, string>()).ToDictionary(entry => entry.Key, entry => entry.Value));

    public static string NextEventId() => $"E{Interlocked.Increment(ref _sequence)}";

    /// <summary>Проверка требования «некорректное событие»: тип, идентификатор объекта и время обязательны.</summary>
    public static bool IsValid(DomainEvent domainEvent, out string error)
    {
        error = string.Empty;

        if (domainEvent is null)
        {
            error = "event is missing";
            return false;
        }

        if (!Enum.IsDefined(domainEvent.EventType))
        {
            error = $"unknown event type '{domainEvent.EventType}'";
            return false;
        }

        if (domainEvent.EntityId <= 0)
        {
            error = $"entity id must be a positive number, got {domainEvent.EntityId}";
            return false;
        }

        if (string.IsNullOrWhiteSpace(domainEvent.EventId))
        {
            error = "event id is missing";
            return false;
        }

        return true;
    }
}

public enum DeliveryOutcome
{
    Delivered,
    Unavailable,
    ConnectionError,
    ProcessingError,
    Dropped
}

/// <summary>Один факт доставки события одному подписчику - источник и для журнала, и для таблицы задержек.</summary>
public sealed record DeliveryRecord(
    string Broker,
    string Subscriber,
    string EventId,
    string EventType,
    DateTime StartedAt,
    DateTime? FinishedAt,
    DeliveryOutcome Outcome,
    int Attempts,
    string? Error,
    string Result)
{
    public int DurationMs => FinishedAt is null ? 0 : (int)(FinishedAt.Value - StartedAt).TotalMilliseconds;

    public string OutcomeText => Outcome switch
    {
        DeliveryOutcome.Delivered => "SUCCESS",
        DeliveryOutcome.Unavailable => "UNAVAILABLE",
        DeliveryOutcome.ConnectionError => "CONNECTION ERROR",
        DeliveryOutcome.ProcessingError => "FAIL",
        _ => "DROPPED"
    };

    /// <summary>
    /// Тот же исход в таблице результатов: ошибка обработки отличается от отказа соединения,
    /// а Attempt N -> FAIL/SUCCESS в журнале остаётся в формулировке из задания.
    /// </summary>
    public string StatusLabel => Outcome == DeliveryOutcome.ProcessingError ? "PROCESSING ERROR" : OutcomeText;
}

/// <summary>Результат публикации: принял ли Broker событие и какие брокеры отказали.</summary>
public sealed record PublicationResult(
    bool IsSuccess,
    string EventId,
    string EventType,
    int EntityId,
    string Message,
    IReadOnlyList<string> AcceptedBy,
    IReadOnlyList<string> RefusedBy)
{
    public static PublicationResult Rejected(DomainEvent domainEvent, string error) =>
        new(false, domainEvent.EventId, domainEvent.EventType.ToString(), domainEvent.EntityId, error,
            Array.Empty<string>(), Array.Empty<string>());
}

/// <summary>
/// Событие плюс все его доставки: из записи собираются и журнал, и таблица задержек Части 5.
/// </summary>
public sealed record EventTrace(
    DomainEvent Event,
    DateTime PublishedAt,
    int PublishMs,
    IReadOnlyList<string> AcceptedBy,
    IReadOnlyList<string> RefusedBy,
    IReadOnlyList<DeliveryRecord> Delivery)
{
    /// <summary>Время доставки: прошло от публикации до момента, когда первый подписчик успешно обработал событие.</summary>
    public int FirstDeliveryMs => Delivery.Count == 0
        ? 0
        : (int)(Delivery.Where(record => record.Outcome == DeliveryOutcome.Delivered && record.FinishedAt is not null)
                .Select(record => record.FinishedAt!.Value)
                .DefaultIfEmpty(PublishedAt)
                .Min() - PublishedAt).TotalMilliseconds;

    public int LastDeliveryMs => Delivery.Count == 0
        ? 0
        : (int)(Delivery.Where(record => record.FinishedAt is not null)
                .Select(record => record.FinishedAt!.Value)
                .DefaultIfEmpty(PublishedAt)
                .Max() - PublishedAt).TotalMilliseconds;

    public int MaxProcessingMs => Delivery.Count == 0 ? 0 : Delivery.Max(record => record.DurationMs);

    public int DeliveredCount => Delivery.Count(record => record.Outcome == DeliveryOutcome.Delivered);
}

/// <summary>Подписчик недоступен: доставка не состоялась, событие остаётся в очереди Broker.</summary>
public sealed class SubscriberUnavailableException(string subscriber, string reason)
    : Exception($"subscriber '{subscriber}' is UNAVAILABLE: {reason}");

/// <summary>Отказ соединения с подписчиком - отдельная ситуация из списка обработки ошибок.</summary>
public sealed class SubscriberConnectionException(string subscriber, string reason)
    : Exception($"connection to subscriber '{subscriber}' failed: {reason}");

/// <summary>Подписчик принял событие, но его обработка завершилась ошибкой.</summary>
public sealed class SubscriberProcessingException(string subscriber, string reason)
    : Exception($"subscriber '{subscriber}' failed to process the event: {reason}");
