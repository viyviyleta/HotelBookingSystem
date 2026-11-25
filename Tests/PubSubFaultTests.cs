using Server.Messaging;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 3 и Часть 5 ЛР №6: подписчик может отказать тремя способами, ошибка одного не мешает
/// остальным, Broker продолжает работать, а событие ждёт повторной доставки в его очереди.
/// </summary>
public sealed class PubSubFaultTests
{
    // ------------------------------------------------ Требование 3: обработка ошибок самого Subscriber

    [Fact]
    public async Task SubscriberBase_HandleAsync_WhenUnavailable_ThrowsUnavailableException()
    {
        var logger = new LoggerSubscriber("Logger", 0);
        logger.MarkUnavailable("logger stopped");

        await Assert.ThrowsAsync<SubscriberUnavailableException>(
            () => logger.HandleAsync(TestPubSub.Event(), "Client 1"));

        Assert.False(logger.IsAvailable);
        Assert.Equal(1, logger.Failed);
    }

    [Fact]
    public async Task SubscriberBase_HandleAsync_WithConnectionFault_ThrowsConnectionException()
    {
        var logger = new LoggerSubscriber("Logger", 0);
        logger.InjectFault(SubscriberFault.Connection, 1, "connection was reset");

        await Assert.ThrowsAsync<SubscriberConnectionException>(
            () => logger.HandleAsync(TestPubSub.Event(), "Client 1"));
    }

    [Fact]
    public async Task SubscriberBase_HandleAsync_WithProcessingFault_ThrowsProcessingException()
    {
        var notification = new NotificationServiceSubscriber("NotificationService", 0);
        notification.InjectFault(SubscriberFault.Processing, 1, "handler threw");

        await Assert.ThrowsAsync<SubscriberProcessingException>(
            () => notification.HandleAsync(TestPubSub.Event(EventType.PaymentCompleted), "Client 1"));
    }

    [Fact]
    public async Task SubscriberBase_HandleAsync_WhenFaultAttemptsAreOver_HandlesEventAgain()
    {
        var logger = new LoggerSubscriber("Logger", 0);
        logger.InjectFault(SubscriberFault.Connection, 1, "connection was reset");

        await Assert.ThrowsAsync<SubscriberConnectionException>(
            () => logger.HandleAsync(TestPubSub.Event(), "Client 1"));

        string result = await logger.HandleAsync(TestPubSub.Event(entityId: 2), "Client 1");

        Assert.Contains("journal record", result);
        Assert.Equal(1, logger.Handled);
    }

    [Fact]
    public async Task SubscriberBase_Restore_ClearsUnavailableStateAndArmedFault()
    {
        var logger = new LoggerSubscriber("Logger", 0);
        logger.MarkUnavailable("logger stopped");
        logger.Restore("started again");

        Assert.True(logger.IsAvailable);

        string result = await logger.HandleAsync(TestPubSub.Event(), "Client 1");

        Assert.Contains("journal record", result);
    }

    // ------------------------- Требование 3: отказ одного Subscriber не мешает остальным и Broker-у

    [Fact]
    public void EventBroker_Accept_WhenOneSubscriberFails_StillDeliversToTheOthers()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        RecordingSubscriber room = TestPubSub.Attach(hub, "broker-1", "RoomService", EventType.BookingCreated);
        room.MarkUnavailable("room service stopped");

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        Assert.Equal([result.EventId], logger.EventIds);
        Assert.Empty(room.EventIds);
    }

    [Fact]
    public void EventBroker_Accept_WhenSubscriberFails_KeepsEventInItsQueue()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        EventBroker broker = TestPubSub.Broker(hub, "broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        logger.MarkUnavailable("logger stopped");

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        Assert.Equal(1, broker.PendingCount);
        Assert.Equal(1, broker.PendingFor("Logger"));
        Assert.Contains(result.EventId, broker.Pending.Single());
    }

    [Fact]
    public void EventBroker_Accept_AfterFailedDelivery_KeepsAcceptingNewEvents()
    {
        var broker = new EventBroker("broker-1");
        var logger = new RecordingSubscriber("Logger");
        broker.Register(logger, "Tester");
        broker.Subscribe(logger, [EventType.BookingCreated], "Tester");

        logger.MarkUnavailable("logger stopped");
        broker.Accept(TestPubSub.Event(), "Client 1");
        broker.WaitUntilIdle(5000);

        logger.Restore("started again");
        int recipients = broker.Accept(TestPubSub.Event(entityId: 2), "Client 1");
        broker.WaitUntilIdle(5000);

        Assert.Equal(1, recipients);
        Assert.True(broker.IsAvailable);
        Assert.Equal(2, broker.Accepted);
        Assert.Equal(2, broker.DeliveryAttempts);
    }

    // ---------------------------------- Требование 3 Части 5: событие не теряется, попытки доставки

    [Fact]
    public void EventBroker_RetryPending_DeliversQueuedEventAfterSubscriberRecovers()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        EventBroker broker = TestPubSub.Broker(hub, "broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        logger.MarkUnavailable("logger stopped");

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);
        logger.Restore("started again");

        IReadOnlyList<DeliveryRecord> recovered = broker.RetryPending("Tester");
        broker.WaitUntilIdle(5000);

        Assert.Equal([result.EventId], recovered.Select(record => record.EventId));
        Assert.Equal([result.EventId], logger.EventIds);
        Assert.Equal(0, broker.PendingCount);
    }

    [Fact]
    public void EventBroker_RetryPending_ThirdAttemptSucceedsAfterTwoFailures()
    {
        var broker = new EventBroker("broker-1") { MaxAttempts = 3 };
        var logger = new RecordingSubscriber("Logger");
        broker.Register(logger, "Tester");
        broker.Subscribe(logger, [EventType.BookingCreated], "Tester");
        logger.InjectFault(SubscriberFault.Processing, 2, "handler fails twice, then recovers");

        DomainEvent domainEvent = TestPubSub.Event();
        broker.Accept(domainEvent, "Client 1");
        broker.WaitUntilIdle(5000);

        IReadOnlyList<DeliveryRecord> recovered = broker.RetryPending("Tester");
        broker.WaitUntilIdle(5000);

        Assert.Single(recovered);
        Assert.Equal([1, 2, 3],
            broker.DeliveriesFor(domainEvent.EventId).Select(record => record.Attempts).Order().ToList());
        Assert.Equal(DeliveryOutcome.Delivered, broker.DeliveriesFor(domainEvent.EventId).MaxBy(
            record => record.Attempts)!.Outcome);
    }

    [Fact]
    public void EventBroker_RetryPending_WhenAttemptsAreExhausted_DropsEventAndEmptiesQueue()
    {
        var broker = new EventBroker("broker-1") { MaxAttempts = 2 };
        var logger = new RecordingSubscriber("Logger");
        broker.Register(logger, "Tester");
        broker.Subscribe(logger, [EventType.BookingCreated], "Tester");
        logger.InjectFault(SubscriberFault.Processing, 5, "handler keeps failing");

        DomainEvent domainEvent = TestPubSub.Event();
        broker.Accept(domainEvent, "Client 1");
        broker.WaitUntilIdle(5000);
        broker.RetryPending("Tester");

        Assert.Equal(0, broker.PendingCount);
        Assert.Empty(logger.EventIds);
        Assert.Contains(DeliveryOutcome.Dropped,
            broker.DeliveriesFor(domainEvent.EventId).Select(record => record.Outcome));
    }

    [Fact]
    public void MessagingHub_RetryPending_EmptiesQueuesOfEveryBroker()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1", "broker-2");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        RecordingSubscriber analytics = TestPubSub.Attach(hub, "broker-2", "AnalyticsService", EventType.BookingCreated);

        logger.MarkUnavailable("stopped");
        analytics.MarkUnavailable("stopped");
        TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        Assert.Equal(2, hub.PendingCount);

        logger.Restore("started");
        analytics.Restore("started");
        HubResult result = hub.RetryPending("Tester");

        Assert.True(result.IsSuccess);
        Assert.Equal(0, hub.PendingCount);
        Assert.Single(logger.EventIds);
        Assert.Single(analytics.EventIds);
    }

    // --------------------------- Требование 5 Части 4: Broker недоступен - у Publisher-а понятный отказ

    [Fact]
    public void EventPublisher_Publish_WhenOneBrokerIsDown_PassesEventToTheLivingOne()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1", "broker-2");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        TestPubSub.Attach(hub, "broker-2", "AnalyticsService", EventType.BookingCreated);
        TestPubSub.Broker(hub, "broker-2").MarkUnavailable("broker process stopped");

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        Assert.True(result.IsSuccess);
        Assert.Single(result.AcceptedBy);
        Assert.Single(result.RefusedBy);
        Assert.Equal([result.EventId], logger.EventIds);
    }

    [Fact]
    public void EventPublisher_Publish_WhenNoBrokerIsUp_ReportsFailureInsteadOfThrowing()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        TestPubSub.Broker(hub, "broker-1").MarkUnavailable("broker process stopped");

        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        Assert.False(result.IsSuccess);
        Assert.Contains("NOT published", result.Message);
        Assert.Equal(1, hub.Publisher.FailedPublications);
    }

    [Fact]
    public void MessagingHub_RestoreBroker_MakesTheBrokerAcceptEventsAgain()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        EventBroker broker = TestPubSub.Broker(hub, "broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        hub.FailBroker("broker-1", "broker process stopped", "Tester");
        PublicationResult refused = hub.Publish(EventType.BookingCreated, 1, "Client 1");

        hub.RestoreBroker("broker-1", "Tester");
        PublicationResult accepted = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 2);

        Assert.False(refused.IsSuccess);
        Assert.True(accepted.IsSuccess);
        Assert.True(broker.IsAvailable);
    }
}
