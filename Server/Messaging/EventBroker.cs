using System.Collections.Concurrent;

namespace Server.Messaging;

public sealed class BrokerUnavailableException(string broker, string reason)
    : Exception($"broker '{broker}' is UNAVAILABLE: {reason}");

public sealed class EventBroker(string name)
{
    private const int KeptReceivedEvents = 50;

    private readonly object _gate = new();
    private readonly Dictionary<string, Registration> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<DomainEvent> _received = new();
    private readonly List<PendingDelivery> _pending = new();
    private readonly List<Task> _inFlight = new();
    private readonly ConcurrentQueue<DeliveryRecord> _deliveries = new();

    private volatile bool _available = true;
    private volatile string _reason = "running";

    private int _accepted;
    private int _totalAttempts;

    public string Name { get; } = name;

    public bool IsAvailable => _available;

    public string Reason => _reason;

    public int Accepted => Volatile.Read(ref _accepted);

    public int DeliveryAttempts => Volatile.Read(ref _totalAttempts);

    public int MaxAttempts { get; init; } = ServerConfig.EventDeliveryAttempts;

    public IReadOnlyList<ISubscriber> Subscribers
    {
        get
        {
            lock (_gate)
                return _subscriptions.Values.OrderBy(registration => registration.Subscriber.Name)
                    .Select(registration => registration.Subscriber)
                    .ToList();
        }
    }

    public IReadOnlyList<DomainEvent> Received
    {
        get
        {
            lock (_gate)
                return _received.ToList();
        }
    }

    public IReadOnlyList<string> Pending
    {
        get
        {
            lock (_gate)
                return _pending.Select(entry =>
                    $"{entry.Subscriber.Name} ← {entry.Event.EventType} #{entry.Event.EventId} " +
                    $"(attempts = {entry.Attempts}, reason = {entry.LastError})").ToList();
        }
    }

    public int PendingFor(string subscriber)
    {
        lock (_gate)
            return _pending.Count(entry =>
                string.Equals(entry.Subscriber.Name, subscriber, StringComparison.OrdinalIgnoreCase));
    }

    public int PendingCount
    {
        get
        {
            lock (_gate)
                return _pending.Count;
        }
    }

    public IReadOnlyList<DeliveryRecord> AllDeliveries => _deliveries.ToList();

    public IReadOnlyList<EventType> TypesOf(string subscriber)
    {
        lock (_gate)
            return _subscriptions.TryGetValue(subscriber, out Registration? registration)
                ? registration.Types.OrderBy(type => type.ToString()).ToList()
                : Array.Empty<EventType>();
    }

    public bool HasSubscriber(string subscriber)
    {
        lock (_gate)
            return _subscriptions.ContainsKey(subscriber);
    }

    /// <summary>Требование 5 Части 1: Broker запоминает, что конкретный подписчик заинтересован в типах событий.</summary>
    public IReadOnlyList<EventType> Subscribe(ISubscriber subscriber, IEnumerable<EventType> types, string clientId)
    {
        var added = new List<EventType>();

        lock (_gate)
        {
            if (!_subscriptions.TryGetValue(subscriber.Name, out Registration? registration))
            {
                registration = new Registration(subscriber);
                _subscriptions[subscriber.Name] = registration;
            }

            foreach (EventType type in types.Where(type => !registration.Types.Contains(type)))
            {
                registration.Types.Add(type);
                added.Add(type);
            }
        }

        string names = string.Join(", ", added.Select(type => type.ToString()));
        PubSubLog.Subscribe(clientId, subscriber.Name, Name, names.Length > 0 ? names : "-");

        return added;
    }

    public bool Unsubscribe(string subscriber, EventType type, string clientId)
    {
        lock (_gate)
        {
            if (!_subscriptions.TryGetValue(subscriber, out Registration? registration) || !registration.Types.Remove(type))
                return false;

            PubSubLog.Subscriber(clientId, subscriber, $"Unsubscribe({type}) on Broker {Name}");
            return true;
        }
    }

    public void Register(ISubscriber subscriber, string clientId)
    {
        lock (_gate)
        {
            _subscriptions[subscriber.Name] = new Registration(subscriber);

            PubSubLog.Subscriber(clientId, subscriber.Name,
                $"REGISTERED on Broker {Name} (kind = {subscriber.Kind}, delay = {subscriber.DelayMs} ms)");
        }
    }

    /// <summary>Подписчиков, которым интересно это событие, определяет Broker - Publisher о них не знает.</summary>
    public IReadOnlyList<ISubscriber> RecipientsOf(EventType type)
    {
        lock (_gate)
            return _subscriptions.Values.Where(registration => registration.Types.Contains(type))
                .Select(registration => registration.Subscriber)
                .OrderBy(subscriber => subscriber.Name)
                .ToList();
    }

    /// <summary>Требование 3 Части 1: принять событие и начать асинхронную доставку всем подходящим подписчикам.</summary>
    public int Accept(DomainEvent domainEvent, string clientId)
    {
        if (!_available)
            throw new BrokerUnavailableException(Name, _reason);

        IReadOnlyList<ISubscriber> recipients = RecipientsOf(domainEvent.EventType);

        Interlocked.Increment(ref _accepted);

        lock (_gate)
        {
            _received.Add(domainEvent);

            // Хранение принятого ограниченно: длинный журнал не должен наращивать память Broker.
            if (_received.Count > KeptReceivedEvents)
                _received.RemoveAt(0);
        }

        PubSubLog.Received(clientId, Name, domainEvent, recipients.Count);

        if (recipients.Count == 0)
            PubSubLog.Queue(clientId, Name,
                $"no subscribers for {domainEvent.EventType} - event received but nobody will process it");

        foreach (ISubscriber subscriber in recipients)
            StartDelivery(clientId, subscriber, domainEvent, attempt: 1);

        return recipients.Count;
    }

    public IReadOnlyList<DeliveryRecord> RetryPending(string clientId)
    {
        List<PendingDelivery> snapshot;

        lock (_gate)
            snapshot = _pending.ToList();

        var recovered = new List<DeliveryRecord>();

        foreach (PendingDelivery entry in snapshot)
        {
            while (true)
            {
                if (entry.Attempts >= MaxAttempts)
                {
                    Drop(clientId, entry);
                    break;
                }

                int attempt = entry.Attempts + 1;
                DeliveryRecord record = DeliverOnce(clientId, entry.Subscriber, entry.Event, attempt);
                entry.Attempts = attempt;

                if (record.Outcome == DeliveryOutcome.Delivered)
                {
                    lock (_gate)
                        _pending.Remove(entry);

                    PubSubLog.Retry(clientId, Name,
                        $"Attempt {attempt} → SUCCESS - {entry.Event.EventType} #{entry.Event.EventId} reached " +
                        $"{entry.Subscriber.Name}, the event was not lost while the subscriber was down");

                    recovered.Add(record);
                    break;
                }

                entry.LastError = record.Error ?? record.OutcomeText;

                PubSubLog.Retry(clientId, Name,
                    $"Attempt {attempt} → {record.OutcomeText} ({entry.LastError}), " +
                    $"{MaxAttempts - attempt} attempt(s) left, the event stays in the queue");
            }
        }

        return recovered;
    }

    private void Drop(string clientId, PendingDelivery entry)
    {
        lock (_gate)
            _pending.Remove(entry);

        PubSubLog.Queue(clientId, Name,
            $"DROP {entry.Event.EventType} #{entry.Event.EventId} for {entry.Subscriber.Name} - " +
            $"{MaxAttempts} attempt(s) failed, the event is lost for this subscriber");

        _deliveries.Enqueue(new DeliveryRecord(Name, entry.Subscriber.Name, entry.Event.EventId,
            entry.Event.EventType.ToString(), DateTime.UtcNow, DateTime.UtcNow,
            DeliveryOutcome.Dropped, entry.Attempts, entry.LastError, "-"));
    }

    /// <summary>Доставка идёт отдельной задачей: сервер не ждёт подписчиков и отвечает клиенту сразу.</summary>
    private void StartDelivery(string clientId, ISubscriber subscriber, DomainEvent domainEvent, int attempt)
    {
        Task delivery = Task.Run(() =>
        {
            DeliveryRecord record = DeliverOnce(clientId, subscriber, domainEvent, attempt);

            if (record.Outcome == DeliveryOutcome.Delivered)
                return;

            PubSubLog.Handled(clientId, subscriber.Name, record.OutcomeText,
                $"{domainEvent.EventType} #{domainEvent.EventId}: {record.Error}");

            lock (_gate)
            {
                _pending.Add(new PendingDelivery(domainEvent, subscriber, attempt, record.Error ?? record.OutcomeText));

                // Требование 2 Части 5: журнал должен показывать, что неотданное событие осталось у Broker-а, а не пропало.
                PubSubLog.Queue(clientId, Name,
                    $"QUEUED {domainEvent.EventType} #{domainEvent.EventId} for {subscriber.Name} - " +
                    $"kept in the broker queue, {PendingCount} delivery(ies) waiting");
            }
        });

        lock (_gate)
        {
            _inFlight.RemoveAll(task => task.IsCompleted);
            _inFlight.Add(delivery);
        }
    }

    private DeliveryRecord DeliverOnce(string clientId, ISubscriber subscriber, DomainEvent domainEvent, int attempt)
    {
        Interlocked.Increment(ref _totalAttempts);

        PubSubLog.Sending(clientId, Name, subscriber.Name);

        DateTime startedAt = DateTime.UtcNow;

        try
        {
            string result = subscriber.HandleAsync(domainEvent, clientId).GetAwaiter().GetResult();

            var record = new DeliveryRecord(Name, subscriber.Name, domainEvent.EventId, domainEvent.EventType.ToString(),
                startedAt, DateTime.UtcNow, DeliveryOutcome.Delivered, attempt, null, result);

            _deliveries.Enqueue(record);
            return record;
        }
        catch (Exception ex)
        {
            DeliveryOutcome outcome = ex switch
            {
                SubscriberUnavailableException => DeliveryOutcome.Unavailable,
                SubscriberConnectionException => DeliveryOutcome.ConnectionError,
                _ => DeliveryOutcome.ProcessingError
            };

            var record = new DeliveryRecord(Name, subscriber.Name, domainEvent.EventId, domainEvent.EventType.ToString(),
                startedAt, DateTime.UtcNow, outcome, attempt, ex.Message, "-");

            _deliveries.Enqueue(record);
            return record;
        }
    }

    public IReadOnlyList<DeliveryRecord> DeliveriesFor(string eventId) =>
        _deliveries.Where(record => record.EventId == eventId).ToList();

    /// <summary>Ожидание нужно только для эксперимента: зафиксировать время завершения всех подписчиков.</summary>
    public bool WaitUntilIdle(int timeoutMs)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);

        while (true)
        {
            Task? pending;

            lock (_gate)
            {
                _inFlight.RemoveAll(task => task.IsCompleted);
                pending = _inFlight.FirstOrDefault();

                if (pending is null)
                    return true;
            }

            int remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;

            if (remaining <= 0 || !pending.Wait(remaining))
                return false;

            lock (_gate)
                _inFlight.Remove(pending);
        }
    }

    public void MarkUnavailable(string reason)
    {
        _reason = reason;
        _available = false;
    }

    public void Restore(string reason)
    {
        _reason = reason;
        _available = true;
    }

    private sealed class Registration(ISubscriber subscriber)
    {
        public ISubscriber Subscriber { get; } = subscriber;

        public HashSet<EventType> Types { get; } = new();
    }

    private sealed class PendingDelivery(DomainEvent domainEvent, ISubscriber subscriber, int attempts, string lastError)
    {
        public DomainEvent Event { get; } = domainEvent;

        public ISubscriber Subscriber { get; } = subscriber;

        public int Attempts { get; set; } = attempts;

        public string LastError { get; set; } = lastError;
    }
}
