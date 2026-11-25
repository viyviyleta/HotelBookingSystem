namespace Server.Messaging;

public sealed class EventPublisher
{
    private readonly IReadOnlyList<EventBroker> _brokers;

    private int _published;
    private int _refused;

    public EventPublisher(IReadOnlyList<EventBroker> brokers)
    {
        _brokers = brokers;
        BrokerNames = brokers.Select(broker => broker.Name).ToList();
    }

    public int Published => Volatile.Read(ref _published);

    public int FailedPublications => Volatile.Read(ref _refused);

    public IReadOnlyList<string> BrokerNames { get; }


    public PublicationResult Publish(DomainEvent domainEvent, string clientId)
    {
        if (!DomainEventFactory.IsValid(domainEvent, out string validationError))
        {
            PubSubLog.PublisherFailed(clientId, "-", $"invalid event - {validationError}");
            Interlocked.Increment(ref _refused);

            return PublicationResult.Rejected(domainEvent, $"publication refused: {validationError}");
        }

        var accepted = new List<string>();
        var refused = new List<string>();

        foreach (EventBroker broker in _brokers)
        {
            PubSubLog.Publish(clientId, domainEvent, broker.Name);

            try
            {
                int recipients = broker.Accept(domainEvent, clientId);
                accepted.Add($"{broker.Name} → {recipients} subscriber(s)");
            }
            catch (Exception ex)
            {
                string reason = ex is BrokerUnavailableException
                    ? ex.Message
                    : $"{ex.GetType().Name}: {ex.Message}";

                refused.Add($"{broker.Name} ({reason})");
                PubSubLog.PublisherFailed(clientId, broker.Name, reason);
            }
        }

        bool isPublished = accepted.Count > 0;

        if (isPublished)
            Interlocked.Increment(ref _published);
        else
            Interlocked.Increment(ref _refused);

        string message = isPublished
            ? $"{domainEvent.EventType} #{domainEvent.EventId} published to {string.Join(", ", accepted)}" +
              (refused.Count == 0 ? string.Empty : $"; refused by {string.Join(", ", refused)}")
            : $"{domainEvent.EventType} #{domainEvent.EventId} was NOT published: {string.Join(", ", refused)}";

        PubSubLog.PublisherResult(clientId, isPublished
            ? $"PUBLISHED {domainEvent.EventType} #{domainEvent.EventId}, brokers: {string.Join(", ", accepted)}" +
              (refused.Count == 0 ? ", none refused" : $", refused: {string.Join(", ", refused)}") +
              "; subscribers process the event asynchronously"
            : $"NOT PUBLISHED {domainEvent.EventType} #{domainEvent.EventId}, no broker accepted the event");

        return new PublicationResult(isPublished, domainEvent.EventId, domainEvent.EventType.ToString(),
            domainEvent.EntityId, message, accepted, refused);
    }
}
