using System.Diagnostics;

namespace Server.Messaging;

/// <summary>Результат служебной операции Pub/Sub: текст ответа сервера и свежий отчёт для таблицы клиента.</summary>
public sealed record HubResult(bool IsSuccess, string Message, object? Report = null, string EventName = "");

/// <summary>
/// Компонент, который сервер поднимает при --pubsub on: два Broker, Publisher и набор подписчиков.
/// Здесь же - операции управления (подписка, отказ, повторная доставка) и отчёты для журнала и таблиц.
/// </summary>
public sealed class MessagingHub
{
    private const int KeptPublishedEvents = 100;

    private readonly object _gate = new();
    private readonly List<PublishedRecord> _events = new();

    public MessagingHub(IReadOnlyList<EventBroker> brokers)
    {
        Brokers = brokers;
        Publisher = new EventPublisher(brokers);
    }

    public EventPublisher Publisher { get; }

    public IReadOnlyList<EventBroker> Brokers { get; }

    public static MessagingHub CreateDefault(string clientId)
    {
        var main = new EventBroker("broker-1");
        var secondary = new EventBroker("broker-2");

        var hub = new MessagingHub(new[] { main, secondary });

        Attach(hub, main, "Logger", "logger", clientId);
        Attach(hub, main, "RoomService", "room", clientId);
        Attach(hub, main, "NotificationService", "notification", clientId);
        Attach(hub, secondary, "AnalyticsService", "analytics", clientId);

        ServerLog.Info(clientId, "→ [PUBSUB] topology: " +
                                 "Publisher → broker-1 (Logger, RoomService, NotificationService), " +
                                 "broker-2 (AnalyticsService)");

        return hub;
    }

    private static void Attach(MessagingHub hub, EventBroker broker, string name, string kind, string clientId)
    {
        ISubscriber subscriber = SubscriberFactory.Create(kind, name)!;
        broker.Register(subscriber, clientId);
        broker.Subscribe(subscriber, SubscriberFactory.DefaultSubscriptions[kind], clientId);
    }

    public IReadOnlyList<ISubscriber> Subscribers => Brokers.SelectMany(broker => broker.Subscribers).ToList();

    public ISubscriber? Find(string name) =>
        Subscribers.FirstOrDefault(subscriber =>
            string.Equals(subscriber.Name, name, StringComparison.OrdinalIgnoreCase));

    public EventBroker? BrokerOf(string name) =>
        Brokers.FirstOrDefault(broker => broker.HasSubscriber(name));

    // ------------------------------------------------------------------ ПУБЛИКАЦИЯ

    public PublicationResult Publish(EventType type, int entityId, string clientId,
        IDictionary<string, string>? data = null) =>
        Publish(DomainEventFactory.Create(type, entityId, clientId, data), clientId);

    /// <summary>Сервер формирует событие, Publisher передаёт его Broker-ам, сервер получает результат публикации.</summary>
    public PublicationResult Publish(DomainEvent domainEvent, string clientId)
    {
        PubSubLog.Formed(clientId, domainEvent);

        DateTime publishedAt = DateTime.UtcNow;

        Stopwatch stopwatch = Stopwatch.StartNew();
        PublicationResult result = Publisher.Publish(domainEvent, clientId);
        stopwatch.Stop();

        lock (_gate)
        {
            _events.Add(new PublishedRecord(domainEvent, publishedAt, (int)stopwatch.ElapsedMilliseconds,
                result.IsSuccess, result.AcceptedBy, result.RefusedBy));

            if (_events.Count > KeptPublishedEvents)
                _events.RemoveAt(0);
        }

        return result;
    }

    public IReadOnlyList<DomainEvent> PublishedEvents
    {
        get
        {
            lock (_gate)
                return _events.Select(record => record.Event).ToList();
        }
    }

    // ------------------------------------------------------- УПРАВЛЕНИЕ ПОДПИСЧИКАМИ

    /// <summary>Часть 4, Требование 1: новый Subscriber появляется во время работы процесса, Publisher не меняется.</summary>
    public HubResult AddSubscriber(string name, string kind, string brokerName, IReadOnlyList<string> eventTypeNames,
        string clientId)
    {
        if (string.IsNullOrWhiteSpace(name))
            return Failed("Invalid data: subscriber name is required");

        if (Find(name) is not null)
            return Failed($"Subscriber '{name}' is already registered (operation would create a duplicate)");

        if (!SubscriberFactory.IsKnownKind(kind))
            return Failed($"Unknown subscriber kind '{kind}'. Supported: logger, room, notification, audit, analytics");

        EventBroker? broker = ResolveBroker(brokerName, out HubResult? refusal);
        if (broker is null)
            return refusal!;

        IReadOnlyList<string> requested = eventTypeNames.Count > 0
            ? eventTypeNames
            : SubscriberFactory.DefaultSubscriptions[kind.Trim().ToLowerInvariant()]
                .Select(type => type.ToString()).ToList();

        var types = new List<EventType>();

        foreach (string requestedType in requested)
        {
            if (!EventTypes.TryParse(requestedType, out EventType type))
                return Failed($"Unknown event type '{requestedType}'. Known types: {string.Join(", ", EventTypes.Names)}");

            types.Add(type);
        }

        ISubscriber subscriber = SubscriberFactory.Create(kind, name)!;

        broker.Register(subscriber, clientId);
        broker.Subscribe(subscriber, types, clientId);

        PubSubLog.Subscriber(clientId, name,
            $"ADDED while the server is running (Broker = {broker.Name}, subscriptions = {string.Join(", ", types)})");

        return new HubResult(true,
            $"Subscriber {name} (kind = {kind}) registered on {broker.Name}, subscribed to {string.Join(", ", types)}",
            Status());
    }

    public HubResult Subscribe(string subscriberName, string eventTypeName, string clientId)
    {
        ISubscriber? subscriber = Find(subscriberName);

        if (subscriber is null)
            return Failed($"Subscriber '{subscriberName}' is not registered. Registered: " +
                          string.Join(", ", Subscribers.Select(entry => entry.Name)));

        if (!EventTypes.TryParse(eventTypeName, out EventType type))
            return Failed($"Unknown event type '{eventTypeName}'. Known types: {string.Join(", ", EventTypes.Names)}");

        EventBroker broker = BrokerOf(subscriberName)!;
        IReadOnlyList<EventType> added = broker.Subscribe(subscriber, new[] { type }, clientId);

        return new HubResult(true,
            added.Count == 0
                ? $"{subscriberName} was already subscribed to {type} on {broker.Name}"
                : $"{subscriberName} → Subscribe({type}) on {broker.Name}",
            Status());
    }

    public HubResult Unsubscribe(string subscriberName, string eventTypeName, string clientId)
    {
        ISubscriber? subscriber = Find(subscriberName);

        if (subscriber is null)
            return Failed($"Subscriber '{subscriberName}' is not registered");

        if (!EventTypes.TryParse(eventTypeName, out EventType type))
            return Failed($"Unknown event type '{eventTypeName}'. Known types: {string.Join(", ", EventTypes.Names)}");

        if (!BrokerOf(subscriberName)!.Unsubscribe(subscriber.Name, type, clientId))
            return Failed($"{subscriberName} has no subscription to {type}");

        return new HubResult(true, $"{subscriberName} → Unsubscribe({type})", Status());
    }

    /// <summary>
    /// Отказ подписчика тремя способами из методички: остановка (состояние), ошибка соединения
    /// и ошибка обработки. Attempts задаёт, сколько попыток подряд завершится неудачей.
    /// </summary>
    public HubResult FailSubscriber(string name, string mode, int attempts, string reason, string clientId)
    {
        ISubscriber? subscriber = Find(name);

        if (subscriber is null)
            return Failed($"Subscriber '{name}' is not registered. Registered: " +
                          string.Join(", ", Subscribers.Select(entry => entry.Name)));

        string cause = string.IsNullOrWhiteSpace(reason)
            ? $"failure injected by {clientId} at {DateTime.UtcNow:HH:mm:ss}"
            : reason;

        string failure;

        switch (mode.Trim().ToLowerInvariant())
        {
            case "unavailable":
            case "stop":
                subscriber.MarkUnavailable(cause);
                failure = $"STOPPED, state = UNAVAILABLE ({cause})";
                break;

            case "connection":
                subscriber.InjectFault(SubscriberFault.Connection, attempts, cause);
                failure = $"CONNECTION FAULT ARMED, the next {attempts} delivery attempt(s) fail with a connection error";
                break;

            case "error":
            case "processing":
                subscriber.InjectFault(SubscriberFault.Processing, attempts, cause);
                failure = $"PROCESSING FAULT ARMED, the next {attempts} delivery attempt(s) fail with a processing error";
                break;

            default:
                return Failed($"Unknown failure mode '{mode}'. Supported: unavailable, connection, processing");
        }

        PubSubLog.Subscriber(clientId, subscriber.Name, failure);

        return new HubResult(true, $"Subscriber {name}: {failure}", Status());
    }

    public HubResult RestoreSubscriber(string name, string clientId)
    {
        ISubscriber? subscriber = Find(name);

        if (subscriber is null)
            return Failed($"Subscriber '{name}' is not registered");

        subscriber.Restore($"restored by {clientId} at {DateTime.UtcNow:HH:mm:ss}");

        int waiting = PendingFor(name);

        PubSubLog.Subscriber(clientId, name,
            waiting == 0
                ? "STARTED - available again"
                : $"STARTED - available again, {waiting} event(s) still waiting in the broker queue");

        return new HubResult(true, $"Subscriber {name}: {Describe(subscriber)}", Status());
    }

    // -------------------------------------------------------------- УПРАВЛЕНИЕ BROKER

    public HubResult FailBroker(string name, string reason, string clientId)
    {
        EventBroker? broker = Brokers.FirstOrDefault(entry =>
            string.Equals(entry.Name, name, StringComparison.OrdinalIgnoreCase));

        if (broker is null)
            return Failed($"Broker '{name}' does not exist. Configured: {string.Join(", ", BrokerNames)}");

        broker.MarkUnavailable(string.IsNullOrWhiteSpace(reason)
            ? $"failure injected by {clientId} at {DateTime.UtcNow:HH:mm:ss}"
            : reason);

        PubSubLog.BrokerState(clientId, broker.Name, "UNAVAILABLE", $"- {broker.Reason}");

        return new HubResult(true, $"Broker {broker.Name} is UNAVAILABLE: {broker.Reason}", Status());
    }

    public HubResult RestoreBroker(string name, string clientId)
    {
        EventBroker? broker = Brokers.FirstOrDefault(entry =>
            string.Equals(entry.Name, name, StringComparison.OrdinalIgnoreCase));

        if (broker is null)
            return Failed($"Broker '{name}' does not exist. Configured: {string.Join(", ", BrokerNames)}");

        broker.Restore($"restored by {clientId} at {DateTime.UtcNow:HH:mm:ss}");

        PubSubLog.BrokerState(clientId, broker.Name, "AVAILABLE", $"- {broker.Reason}");

        return new HubResult(true, $"Broker {broker.Name} is AVAILABLE again", Status());
    }

    /// <summary>Часть 5, Требование 3: повторные попытки доставки для всех событий, оставшихся в очередях.</summary>
    public HubResult RetryPending(string clientId)
    {
        var recovered = new List<DeliveryRecord>();
        int queued = PendingCount;

        PubSubLog.Retry(clientId, "-", $"RETRY START - {queued} delivery(s) waiting in the broker queues");

        foreach (EventBroker broker in Brokers)
            recovered.AddRange(broker.RetryPending(clientId));

        int dropped = queued - recovered.Count;

        PubSubLog.Retry(clientId, "-",
            $"RETRY DONE - {recovered.Count} event(s) reached their subscribers, {dropped} remained undelivered, " +
            $"queue left = {PendingCount}");

        return new HubResult(recovered.Count > 0,
            $"Redelivery: {recovered.Count} recovered, {dropped} not delivered this pass, queue = {PendingCount}",
            Status());
    }

    // ---------------------------------------------------------------------- ОТЧЁТЫ

    public int PendingCount => Brokers.Sum(broker => broker.PendingCount);

    public int DeliveredCount => Brokers.Sum(broker => broker.AllDeliveries.Count(record =>
        record.Outcome == DeliveryOutcome.Delivered));

    public int FailedDeliveries => Brokers.Sum(broker => broker.AllDeliveries.Count(record =>
        record.Outcome != DeliveryOutcome.Delivered));

    public IReadOnlyList<string> BrokerNames => Brokers.Select(broker => broker.Name).ToList();

    public IReadOnlyList<DeliveryRecord> DeliveriesFor(string eventId) =>
        Brokers.SelectMany(broker => broker.DeliveriesFor(eventId)).ToList();

    public int HandledBy(string subscriberName) => Find(subscriberName)?.Handled ?? 0;

    public bool Received(string subscriberName, string eventId) =>
        DeliveriesFor(eventId).Any(record =>
            string.Equals(record.Subscriber, subscriberName, StringComparison.OrdinalIgnoreCase) &&
            record.Outcome == DeliveryOutcome.Delivered);

    /// <summary>Ожидание нужно только эксперименту: зафиксировать, когда завершат обработку все подписчики.</summary>
    public bool WaitForDeliveries(string clientId)
    {
        bool idle = Brokers.All(broker => broker.WaitUntilIdle(ServerConfig.EventWaitTimeoutMs));

        PubSubLog.Statistics(clientId, idle
            ? "all subscribers finished processing the queued events"
            : "some subscribers are still processing - the wait timed out");

        return idle;
    }

    public PubSubStatusReport Status()
    {
        List<BrokerStatus> brokers = Brokers.Select(broker => new BrokerStatus(
            broker.Name,
            broker.IsAvailable ? "AVAILABLE" : "UNAVAILABLE",
            broker.Reason,
            broker.Accepted,
            broker.DeliveryAttempts,
            broker.PendingCount,
            broker.Subscribers.Select(subscriber => subscriber.Name).ToList(),
            broker.Pending)).ToList();

        List<SubscriberStatus> subscribers = Brokers.SelectMany(broker => broker.Subscribers.Select(subscriber =>
            new SubscriberStatus(
                subscriber.Name,
                subscriber.Kind,
                broker.Name,
                subscriber.IsAvailable ? "AVAILABLE" : "UNAVAILABLE",
                subscriber.Reason,
                subscriber.DelayMs,
                broker.TypesOf(subscriber.Name).Select(type => type.ToString()).ToList(),
                subscriber.Handled,
                subscriber.Failed,
                broker.PendingFor(subscriber.Name),
                subscriber.LastResults))).ToList();

        string brokerStates = string.Join(", ", brokers.Select(broker => $"{broker.Name}: {broker.State}"));

        string summary = $"brokers = {brokers.Count} ({brokerStates}), subscribers = {subscribers.Count}, " +
            $"published events = {Publisher.Published}, refused publications = {Publisher.FailedPublications}, " +
            $"delivered = {DeliveredCount}, undelivered = {FailedDeliveries}, queued = {PendingCount}";

        return new PubSubStatusReport(true, summary, brokers, subscribers, Publisher.Published,
            Publisher.FailedPublications, ServerConfig.EventDeliveryAttempts);
    }

    public EventStatisticsReport Statistics(string clientId)
    {
        List<PublishedRecord> records;

        lock (_gate)
            records = _events.ToList();

        List<EventTiming> events = records.Select(record =>
        {
            IReadOnlyList<DeliveryRecord> deliveries = DeliveriesFor(record.Event.EventId);

            EventTrace trace = new(record.Event, record.PublishedAt, record.PublishMs,
                record.AcceptedBy, record.RefusedBy, deliveries);

            List<SubscriberTiming> timings = deliveries.Select(delivery => new SubscriberTiming(
                delivery.Subscriber, delivery.Broker, delivery.StatusLabel, delivery.Attempts,
                delivery.DurationMs, $"{(delivery.StartedAt - record.PublishedAt).TotalMilliseconds:+0;-0;0} ms",
                delivery.FinishedAt is null
                    ? "-"
                    : $"{(delivery.FinishedAt.Value - record.PublishedAt).TotalMilliseconds:+0;-0;0} ms",
                delivery.Error, delivery.Result)).ToList();

            return new EventTiming(record.Event.EventId, record.Event.EventType.ToString(), record.Event.EntityId,
                record.Event.ClientId, record.PublishedAt.ToString("HH:mm:ss.fff"), record.PublishMs,
                trace.FirstDeliveryMs, trace.MaxProcessingMs, trace.LastDeliveryMs,
                record.IsPublished
                    ? $"accepted by {string.Join(", ", record.AcceptedBy)}"
                    : $"refused by {string.Join(", ", record.RefusedBy)}",
                timings);
        }).ToList();

        string summary = $"events = {events.Count}, deliveries = {DeliveredCount}, " +
                         $"undelivered = {FailedDeliveries}, queued = {PendingCount}, " +
                         $"publish time ≈ {events.Select(entry => entry.PublishMs).DefaultIfEmpty(0).Max()} ms, " +
                         $"all subscribers done ≈ {events.Select(entry => entry.AllDoneMs).DefaultIfEmpty(0).Max()} ms";

        PubSubLog.Statistics(clientId, summary);

        return new EventStatisticsReport(events.Count, DeliveredCount, FailedDeliveries, summary, events);
    }

    private int PendingFor(string subscriberName) => Brokers.Sum(broker => broker.PendingFor(subscriberName));

    private static string Describe(ISubscriber subscriber) =>
        $"{(subscriber.IsAvailable ? "AVAILABLE" : "UNAVAILABLE")} ({subscriber.Reason})";

    private EventBroker? ResolveBroker(string brokerName, out HubResult? refusal)
    {
        refusal = null;

        if (string.IsNullOrWhiteSpace(brokerName))
            return Brokers.FirstOrDefault();

        EventBroker? broker = Brokers.FirstOrDefault(entry =>
            string.Equals(entry.Name, brokerName.Trim(), StringComparison.OrdinalIgnoreCase));

        if (broker is null)
            refusal = Failed($"Broker '{brokerName}' does not exist. Configured: {string.Join(", ", BrokerNames)}");

        return broker;
    }

    private static HubResult Failed(string message) => new(false, message);

    private sealed record PublishedRecord(
        DomainEvent Event,
        DateTime PublishedAt,
        int PublishMs,
        bool IsPublished,
        IReadOnlyList<string> AcceptedBy,
        IReadOnlyList<string> RefusedBy);
}
