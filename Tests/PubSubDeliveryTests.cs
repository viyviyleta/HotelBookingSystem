using Server.Messaging;
using Xunit;

namespace Tests;

/// <summary>
/// Части 1-2 ЛР №6: модель события, Publisher, Broker, Subscriber и доставка только тем,
/// кто подписался на тип события.
/// </summary>
public sealed class PubSubDeliveryTests
{
    // ------------------------------------- Требование 1: модель события (тип, объект, время, данные)

    [Fact]
    public void DomainEventFactory_Create_FillsTypeEntityIdTimeAndData()
    {
        DomainEvent domainEvent = DomainEventFactory.Create(EventType.BookingCreated, 12, "Client 1",
            new Dictionary<string, string> { ["room"] = "101" });

        Assert.Equal(EventType.BookingCreated, domainEvent.EventType);
        Assert.Equal(12, domainEvent.EntityId);
        Assert.Equal("Client 1", domainEvent.ClientId);
        Assert.Equal("101", domainEvent.Data["room"]);
        Assert.True(domainEvent.Timestamp > DateTime.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public void DomainEventFactory_Create_GivesEveryEventItsOwnIdentifier()
    {
        DomainEvent first = TestPubSub.Event();
        DomainEvent second = TestPubSub.Event();

        Assert.NotEqual(first.EventId, second.EventId);
    }

    [Fact]
    public void DomainEventFactory_IsValid_RefusesEventWithoutEntityId()
    {
        DomainEvent broken = TestPubSub.Event(EventType.PaymentCompleted, entityId: 0);

        Assert.False(DomainEventFactory.IsValid(broken, out string error));
        Assert.Contains("positive", error);
    }

    // ------------------------------------------------ Требование 2-3: Publisher передаёт событие Broker-у

    [Fact]
    public void EventPublisher_Publish_HandsTheSameEventToEveryBroker()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1", "broker-2");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        RecordingSubscriber analytics = TestPubSub.Attach(hub, "broker-2", "AnalyticsService", EventType.BookingCreated);

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 5);

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.AcceptedBy.Count);
        Assert.Equal(result.EventId, Assert.Single(logger.EventIds));
        Assert.Equal(result.EventId, Assert.Single(analytics.EventIds));
    }

    [Fact]
    public void EventPublisher_Publish_CountsAcceptedPublicationsOnly()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);
        TestPubSub.PublishAndWait(hub, EventType.PaymentCompleted, 2);
        TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 0);

        Assert.Equal(2, hub.Publisher.Published);
        Assert.Equal(1, hub.Publisher.FailedPublications);
    }

    // ------------------------------------------------------ Требование 4: регистрация Subscriber-а

    [Fact]
    public void EventBroker_Subscribe_KeepsSubscriberAndItsEventTypes()
    {
        var broker = new EventBroker("broker-1");
        var logger = new RecordingSubscriber("Logger");

        broker.Register(logger, "Tester");
        IReadOnlyList<EventType> added =
            broker.Subscribe(logger, [EventType.BookingCreated, EventType.PaymentCompleted], "Tester");

        Assert.Equal(2, added.Count);
        Assert.True(broker.HasSubscriber("Logger"));
        Assert.Equal([EventType.BookingCreated, EventType.PaymentCompleted], broker.TypesOf("Logger"));
    }

    [Fact]
    public void EventBroker_Subscribe_DoesNotAddTheSameTypeTwice()
    {
        var broker = new EventBroker("broker-1");
        var logger = new RecordingSubscriber("Logger");

        broker.Register(logger, "Tester");
        broker.Subscribe(logger, [EventType.BookingCreated], "Tester");
        IReadOnlyList<EventType> repeated = broker.Subscribe(logger, [EventType.BookingCreated], "Tester");

        Assert.Empty(repeated);
        Assert.Single(broker.TypesOf("Logger"));
    }

    // ------------------------------------------ Требование 5: эксперимент Publisher → Broker → Subscriber

    [Fact]
    public void EventBroker_Accept_DeliversEventToSubscribedSubscriber()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 7);

        DomainEvent delivered = Assert.Single(logger.Received);
        Assert.Equal(result.EventId, delivered.EventId);
        Assert.Equal(7, delivered.EntityId);
        Assert.Equal("Client 1", delivered.ClientId);
    }

    [Fact]
    public void EventBroker_Accept_SkipsSubscriberWithoutSubscriptionOnThatType()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber room = TestPubSub.Attach(hub, "broker-1", "RoomService", EventType.BookingCreated);

        TestPubSub.PublishAndWait(hub, EventType.PaymentCompleted, 3);

        Assert.Empty(room.Received);
        Assert.Equal(0, room.Handled);
    }

    [Fact]
    public void EventBroker_Accept_DeliversOneEventToEverySubscriberOfItsType()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger",
            EventType.BookingCreated, EventType.BookingCancelled, EventType.PaymentCompleted);
        RecordingSubscriber notification = TestPubSub.Attach(hub, "broker-1", "NotificationService",
            EventType.BookingCreated, EventType.PaymentCompleted);
        RecordingSubscriber room = TestPubSub.Attach(hub, "broker-1", "RoomService", EventType.BookingCreated);

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 11);

        Assert.Equal([result.EventId], logger.EventIds);
        Assert.Equal([result.EventId], notification.EventIds);
        Assert.Equal([result.EventId], room.EventIds);
        Assert.Equal(3, TestPubSub.Broker(hub, "broker-1").RecipientsOf(EventType.BookingCreated).Count);
    }

    [Fact]
    public void EventBroker_Accept_DeliversEachEventTypeToTheSameSubscriber()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger",
            EventType.BookingCreated, EventType.BookingCancelled, EventType.PaymentCompleted);

        PublicationResult created = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);
        PublicationResult cancelled = TestPubSub.PublishAndWait(hub, EventType.BookingCancelled, 2);
        PublicationResult paid = TestPubSub.PublishAndWait(hub, EventType.PaymentCompleted, 3);

        Assert.Equal([created.EventId, cancelled.EventId, paid.EventId], logger.EventIds);
        Assert.Equal(
            [EventType.BookingCreated, EventType.BookingCancelled, EventType.PaymentCompleted],
            logger.EventTypes);
    }

    [Fact]
    public void EventBroker_Accept_KeepsEventWhenNobodySubscribedToItsType()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        EventBroker broker = TestPubSub.Broker(hub, "broker-1");
        TestPubSub.Attach(hub, "broker-1", "RoomService", EventType.BookingCreated);

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.PaymentCompleted, 4);

        Assert.True(result.IsSuccess);
        Assert.Empty(broker.RecipientsOf(EventType.PaymentCompleted));
        Assert.Empty(broker.DeliveriesFor(result.EventId));
        Assert.Contains(broker.Received, entry => entry.EventId == result.EventId);
    }
}
