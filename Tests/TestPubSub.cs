using Server;
using Server.Messaging;

namespace Tests;

/// <summary>
/// Общие инструменты тестов ЛР №6. Broker доставляет событие в отдельной задаче, поэтому каждая
/// проверка сначала ждёт, пока Broker закончит доставки, и только потом читает результат.
/// </summary>
internal static class TestPubSub
{
    /// <summary>Имена подписчиков из методички: тесты ставят им минимальную задержку обработки.</summary>
    private static readonly string[] KnownSubscribers =
        ["Logger", "RoomService", "NotificationService", "AuditService", "AnalyticsService", "ReportService"];

    /// <summary>
    /// Задержки из Части 3 (1/3/2 секунды) принадлежат конфигурации сервера, а не логике Broker-а.
    /// В тестах они уменьшены, иначе прогон всей проверки занял бы минуты.
    /// </summary>
    public static void FastDelays()
    {
        foreach (string name in KnownSubscribers)
            ServerConfig.SubscriberDelaysMs[name] = 5;

        ServerConfig.EventDeliveryAttempts = 3;
        ServerConfig.EventWaitTimeoutMs = 10000;
    }

    /// <summary>Broker-ы без подписчиков: тест сам регистрирует ровно тех, кто нужен проверке.</summary>
    public static MessagingHub Hub(params string[] brokerNames)
    {
        FastDelays();

        IReadOnlyList<EventBroker> brokers = brokerNames
            .Select(name => new EventBroker(name))
            .ToList();

        return new MessagingHub(brokers);
    }

    public static EventBroker Broker(MessagingHub hub, string name) =>
        hub.Brokers.First(broker => broker.Name == name);

    /// <summary>Регистрация подписчика-двойника и его подписок на типы событий.</summary>
    public static RecordingSubscriber Attach(MessagingHub hub, string brokerName, string subscriberName,
        params EventType[] types)
    {
        var subscriber = new RecordingSubscriber(subscriberName);
        EventBroker broker = Broker(hub, brokerName);

        broker.Register(subscriber, "Tester");
        broker.Subscribe(subscriber, types, "Tester");

        return subscriber;
    }

    /// <summary>Публикация с ожиданием асинхронных доставок: дальше тест читает уже готовый результат.</summary>
    public static PublicationResult PublishAndWait(MessagingHub hub, EventType type, int entityId,
        string clientId = "Client 1")
    {
        PublicationResult result = hub.Publish(type, entityId, clientId);
        hub.WaitForDeliveries("Tester");

        return result;
    }

    public static DomainEvent Event(EventType type = EventType.BookingCreated, int entityId = 1,
        string clientId = "Client 1") =>
        DomainEventFactory.Create(type, entityId, clientId);
}

/// <summary>
/// Подписчик-двойник: помнит дошедшие до него события и отказывает тем способом, который задал тест.
/// Нужен, чтобы проверять маршрутизацию Broker-а, а не поведение реальных подписчиков предметной области.
/// </summary>
internal sealed class RecordingSubscriber(string name) : ISubscriber
{
    private readonly object _gate = new();
    private readonly List<DomainEvent> _received = new();

    private volatile bool _available = true;
    private volatile string _reason = "running";
    private SubscriberFault _fault;
    private int _faultsRemaining;
    private int _failed;

    public string Name { get; } = name;

    public string Kind { get; } = "recording";

    public int DelayMs { get; } = 0;

    public bool IsAvailable => _available;

    public string Reason => _reason;

    public int Handled => Received.Count;

    public int Failed => Volatile.Read(ref _failed);

    public IReadOnlyList<string> LastResults =>
        Received.Select(domainEvent => $"{domainEvent.EventType} #{domainEvent.EventId}").ToList();

    /// <summary>События в порядке обработки: по ним тест видит, дошло ли событие и какое именно.</summary>
    public IReadOnlyList<DomainEvent> Received
    {
        get
        {
            lock (_gate)
                return _received.ToList();
        }
    }

    public IReadOnlyList<string> EventIds => Received.Select(domainEvent => domainEvent.EventId).ToList();

    public IReadOnlyList<EventType> EventTypes => Received.Select(domainEvent => domainEvent.EventType).ToList();

    public void MarkUnavailable(string reason)
    {
        _available = false;
        _reason = reason;
    }

    public void Restore(string reason)
    {
        _available = true;
        _reason = reason;
        _fault = SubscriberFault.None;
        Volatile.Write(ref _faultsRemaining, 0);
    }

    public void InjectFault(SubscriberFault fault, int attempts, string reason)
    {
        _fault = fault;
        Volatile.Write(ref _faultsRemaining, Math.Max(0, attempts));
        _reason = reason;
    }

    public Task<string> HandleAsync(DomainEvent domainEvent, string clientId)
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

        lock (_gate)
            _received.Add(domainEvent);

        return Task.FromResult($"recorded {domainEvent.EventType} #{domainEvent.EventId}");
    }
}
