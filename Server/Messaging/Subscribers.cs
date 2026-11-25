namespace Server.Messaging;

/// <summary>Компонент, который подписывается на типы событий и обрабатывает их независимо от Publisher.</summary>
public interface ISubscriber
{
    string Name { get; }

    string Kind { get; }

    int DelayMs { get; }

    bool IsAvailable { get; }

    string Reason { get; }

    int Handled { get; }

    int Failed { get; }

    IReadOnlyList<string> LastResults { get; }

    void MarkUnavailable(string reason);

    void Restore(string reason);

    /// <summary>Сколько подряд доставок завершатся ошибкой - нужно для показа повторной доставки.</summary>
    void InjectFault(SubscriberFault fault, int attempts, string reason);

    Task<string> HandleAsync(DomainEvent domainEvent, string clientId);
}

public enum SubscriberFault
{
    None,
    Connection,
    Processing
}

public abstract class SubscriberBase(string name, string kind, int delayMs) : ISubscriber
{
    private readonly object _gate = new();
    private readonly List<string> _results = new();

    private volatile bool _available = true;
    private volatile string _reason = "running";
    private int _faultsRemaining;
    private SubscriberFault _fault = SubscriberFault.None;
    private int _handled;
    private int _failed;

    public string Name { get; } = name;

    public string Kind { get; } = kind;

    public int DelayMs { get; } = delayMs;

    public bool IsAvailable => _available;

    public string Reason => _reason;

    public int Handled => Volatile.Read(ref _handled);

    public int Failed => Volatile.Read(ref _failed);

    public IReadOnlyList<string> LastResults
    {
        get
        {
            lock (_gate)
                return _results.TakeLast(5).ToList();
        }
    }

    public void MarkUnavailable(string reason)
    {
        _available = false;
        _reason = reason;
    }

    public void Restore(string reason)
    {
        _available = true;
        _reason = reason;
        Interlocked.Exchange(ref _faultsRemaining, 0);
        _fault = SubscriberFault.None;
    }

    public void InjectFault(SubscriberFault fault, int attempts, string reason)
    {
        _fault = fault;
        Interlocked.Exchange(ref _faultsRemaining, Math.Max(0, attempts));
        _reason = reason;
    }

    /// <summary>
    /// Отказ проверяется до задержки: недоступный подписчик не должен удерживать доставку на секунды.
    /// Успешная обработка, наоборот, идёт с искусственной задержкой, чтобы показать независимость подписчиков.
    /// </summary>
    public async Task<string> HandleAsync(DomainEvent domainEvent, string clientId)
    {
        if (!_available)
        {
            Interlocked.Increment(ref _failed);
            throw new SubscriberUnavailableException(Name, _reason);
        }

        if (Volatile.Read(ref _faultsRemaining) > 0 && Interlocked.Decrement(ref _faultsRemaining) >= 0)
        {
            Interlocked.Increment(ref _failed);

            throw _fault == SubscriberFault.Connection
                ? new SubscriberConnectionException(Name, _reason)
                : new SubscriberProcessingException(Name, _reason);
        }

        await Task.Delay(DelayMs);

        string result = Process(domainEvent);
        Interlocked.Increment(ref _handled);

        lock (_gate)
        {
            _results.Add($"{domainEvent.EventType} #{domainEvent.EventId} → {result}");
        }

        PubSubLog.Handled(clientId, Name, "SUCCESS",
            $"{domainEvent.EventType} #{domainEvent.EventId} ({DelayMs} ms): {result}");

        return result;
    }

    protected abstract string Process(DomainEvent domainEvent);

    protected static string Data(DomainEvent domainEvent, string key) =>
        domainEvent.Data.TryGetValue(key, out string? value) ? value : "-";
}

/// <summary>Logger: записывает все события, ничего не меняя в предметной области.</summary>
public sealed class LoggerSubscriber(string name, int delayMs) : SubscriberBase(name, "logger", delayMs)
{
    protected override string Process(DomainEvent domainEvent) =>
        $"journal record: {domainEvent.EventType} #{domainEvent.EventId}, entity = {domainEvent.EntityId}, " +
        $"client = {domainEvent.ClientId}, time = {domainEvent.Timestamp:HH:mm:ss.fff}";
}

/// <summary>
/// RoomService - деловой подписчик варианта 2: по событию сам отмечает номер занятым в собственном
/// реестре. К базе он не обращается и данных предметной области не изменяет: состояние строится из событий.
/// </summary>
public sealed class RoomServiceSubscriber(string name, int delayMs) : SubscriberBase(name, "room", delayMs)
{
    private readonly Dictionary<int, string> _occupancy = new();

    public IReadOnlyDictionary<int, string> Occupancy => _occupancy;

    protected override string Process(DomainEvent domainEvent)
    {
        if (!int.TryParse(Data(domainEvent, "room"), out int roomId))
            return "event has no room id - nothing to mark";

        string state = domainEvent.EventType switch
        {
            EventType.BookingCreated => "BUSY",
            EventType.BookingCancelled => "FREE",
            _ => _occupancy.TryGetValue(roomId, out string? known) ? known : "UNKNOWN"
        };

        lock (_occupancy)
            _occupancy[roomId] = state;

        return $"RoomService register: room {roomId} → {state} (booking {domainEvent.EntityId})";
    }
}

/// <summary>NotificationService: формирует уведомление гостю по созданной брони и по полученной оплате.</summary>
public sealed class NotificationServiceSubscriber(string name, int delayMs) : SubscriberBase(name, "notification", delayMs)
{
    protected override string Process(DomainEvent domainEvent) =>
        domainEvent.EventType switch
        {
            EventType.BookingCreated =>
                $"notification to guest {Data(domainEvent, "guest")}: booking {domainEvent.EntityId} of room " +
                $"{Data(domainEvent, "room")} is confirmed, total = {Data(domainEvent, "total")}",
            EventType.PaymentCompleted =>
                $"notification to guest {Data(domainEvent, "guest")}: payment for booking {domainEvent.EntityId} " +
                $"is received, amount = {Data(domainEvent, "amount")}",
            _ => $"notification skipped for {domainEvent.EventType}"
        };
}

/// <summary>AuditService - подписчик, добавляемый во время работы процесса (Часть 4, Требование 1).</summary>
public sealed class AuditServiceSubscriber(string name, int delayMs) : SubscriberBase(name, "audit", delayMs)
{
    protected override string Process(DomainEvent domainEvent) =>
        $"audit entry: {domainEvent.EventType} of booking {domainEvent.EntityId} by {domainEvent.ClientId} " +
        $"at {domainEvent.Timestamp:HH:mm:ss.fff}";
}

/// <summary>
/// AnalyticsService (Часть 5, Требование 4): только собирает статистику по событиям,
/// ничего не изменяя ни в базе, ни в состоянии других подписчиков.
/// </summary>
public sealed class AnalyticsServiceSubscriber(string name, int delayMs) : SubscriberBase(name, "analytics", delayMs)
{
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _counters = new();
    private readonly HashSet<string> _clients = new();

    public IReadOnlyDictionary<string, int> Counters
    {
        get
        {
            lock (_gate)
                return _counters.ToDictionary(entry => entry.Key, entry => entry.Value);
        }
    }

    public int DistinctClients
    {
        get
        {
            lock (_gate)
                return _clients.Count;
        }
    }

    protected override string Process(DomainEvent domainEvent)
    {
        lock (_gate)
        {
            _counters[domainEvent.EventType.ToString()] =
                _counters.TryGetValue(domainEvent.EventType.ToString(), out int count) ? count + 1 : 1;
            _clients.Add(domainEvent.ClientId);

            string counters = string.Join(", ", _counters.OrderBy(entry => entry.Key)
                .Select(entry => $"{entry.Key} = {entry.Value}"));

            return $"analytics: {counters}, clients = {_clients.Count}";
        }
    }
}

public static class SubscriberFactory
{
    /// <summary>Подписки по умолчанию - ровно как в примере методички для варианта с бронированием отелей.</summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<EventType>> DefaultSubscriptions =
        new Dictionary<string, IReadOnlyList<EventType>>
        {
            ["logger"] = [EventType.BookingCreated, EventType.BookingCancelled, EventType.PaymentCompleted],
            ["room"] = [EventType.BookingCreated],
            ["notification"] = [EventType.BookingCreated, EventType.PaymentCompleted],
            ["audit"] = [EventType.BookingCreated, EventType.BookingCancelled],
            ["analytics"] = [EventType.BookingCreated, EventType.BookingCancelled, EventType.PaymentCompleted]
        };

    /// <summary>Неизвестный тип подписчика - это отказ операции, а не исключение в сервере.</summary>
    public static ISubscriber? Create(string kind, string name) =>
        kind.Trim().ToLowerInvariant() switch
        {
            "logger" => new LoggerSubscriber(name, Delay(name)),
            "room" => new RoomServiceSubscriber(name, Delay(name)),
            "notification" => new NotificationServiceSubscriber(name, Delay(name)),
            "audit" => new AuditServiceSubscriber(name, Delay(name)),
            "analytics" => new AnalyticsServiceSubscriber(name, Delay(name)),
            _ => null
        };

    public static bool IsKnownKind(string kind) => DefaultSubscriptions.ContainsKey(kind.Trim().ToLowerInvariant());

    private static int Delay(string name) =>
        ServerConfig.SubscriberDelaysMs.TryGetValue(name, out int delay) ? delay : 1000;
}
