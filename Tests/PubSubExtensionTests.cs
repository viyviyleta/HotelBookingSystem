using System.Reflection;
using System.Text.Json;
using Server;
using Server.Messaging;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Части 4-5 ЛР №6 и связка с предметной областью: Subscriber появляется во время работы процесса,
/// Publisher от него не зависит, аналитика собирает статистику не меняя данные, а события рождаются
/// из реальных операций бронирования.
/// </summary>
public sealed class PubSubExtensionTests : IDisposable
{
    public void Dispose()
    {
        TestDatabase.Cleanup();
    }

    // ---------------------------------- Требование 1 Части 4: новый Subscriber во время работы сервера

    [Fact]
    public void MessagingHub_AddSubscriber_RegistersItOnTheRunningBroker()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        HubResult result = hub.AddSubscriber("AuditService", "audit", "broker-1",
            new[] { "BookingCreated", "BookingCancelled" }, "Tester");

        Assert.True(result.IsSuccess);
        Assert.NotNull(hub.Find("AuditService"));
        Assert.Equal([EventType.BookingCancelled, EventType.BookingCreated],
            TestPubSub.Broker(hub, "broker-1").TypesOf("AuditService"));
    }

    [Fact]
    public void MessagingHub_AddSubscriber_ReceivesEventsPublishedAfterItAppeared()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        PublicationResult before = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        hub.AddSubscriber("AuditService", "audit", "broker-1", new[] { "BookingCreated" }, "Tester");

        PublicationResult after = TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 2);

        Assert.False(hub.Received("AuditService", before.EventId));
        Assert.True(hub.Received("AuditService", after.EventId));
        Assert.Equal(2, logger.Handled);
    }

    // ------------------------- Требование 2 Части 4: Publisher знает только Broker-ы, а не подписчиков

    [Fact]
    public void EventPublisher_DependsOnBrokersOnly_SoNewSubscribersChangeNothingInThePublisher()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        EventPublisher publisher = hub.Publisher;

        hub.AddSubscriber("AuditService", "audit", "broker-1", new[] { "BookingCancelled" }, "Tester");

        Assert.Same(publisher, hub.Publisher);

        // Типы, через которые Publisher обращается к механизму доставки: Broker-ы и ничего о подписчиках.
        Type[] dependencies = DependenciesOfPublisher();

        Assert.Contains(typeof(IReadOnlyList<EventBroker>), dependencies);
        Assert.All(dependencies, type => Assert.False(typeof(ISubscriber).IsAssignableFrom(type),
            $"Publisher holds '{type}', so a new Subscriber would require changing it"));
    }

    private static Type[] DependenciesOfPublisher() =>
        typeof(EventPublisher)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .SelectMany(field => new[] { field.FieldType }.Concat(field.FieldType.GetGenericArguments()))
            .Distinct()
            .ToArray();

    // ---------------------------------- Требование 5 Части 4: ошибки управления возвращаются ответом

    [Fact]
    public void MessagingHub_AddSubscriber_WithTheSameName_RefusesDuplicate()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        HubResult result = hub.AddSubscriber("Logger", "logger", "broker-1",
            new[] { "BookingCreated" }, "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("already registered", result.Message);
    }

    [Fact]
    public void MessagingHub_AddSubscriber_WithUnknownKind_ReportsSupportedKinds()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");

        HubResult result = hub.AddSubscriber("TeleportService", "teleport", "broker-1",
            new[] { "BookingCreated" }, "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("Unknown subscriber kind", result.Message);
    }

    [Fact]
    public void MessagingHub_Subscribe_WithUnknownEventType_ReportsKnownTypes()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        HubResult result = hub.Subscribe("Logger", "RoomCleaned", "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("Unknown event type", result.Message);
    }

    [Fact]
    public void MessagingHub_Unsubscribe_RemovesThatTypeFromLaterDeliveries()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger",
            EventType.BookingCreated, EventType.PaymentCompleted);

        hub.Unsubscribe("Logger", "PaymentCompleted", "Tester");
        PublicationResult result = TestPubSub.PublishAndWait(hub, EventType.PaymentCompleted, 4);

        Assert.True(result.IsSuccess);
        Assert.Empty(logger.EventIds);
    }

    [Fact]
    public void MessagingHub_Unsubscribe_WithoutSuchSubscription_ReportsIt()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "RoomService", EventType.BookingCreated);

        HubResult result = hub.Unsubscribe("RoomService", "PaymentCompleted", "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("has no subscription", result.Message);
    }

    [Fact]
    public void MessagingHub_FailSubscriber_WithUnknownName_ReportsRegisteredSubscribers()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        HubResult result = hub.FailSubscriber("GhostService", "unavailable", 1, "-", "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("is not registered", result.Message);
        Assert.Contains("Logger", result.Message);
    }

    [Fact]
    public void MessagingHub_FailSubscriber_WithUnknownMode_ReportsSupportedModes()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);

        HubResult result = hub.FailSubscriber("Logger", "explode", 1, "-", "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("Unknown failure mode", result.Message);
    }

    [Fact]
    public void MessagingHub_Status_ReportsBrokerStateAndSubscriberPending()
    {
        MessagingHub hub = TestPubSub.Hub("broker-1");
        RecordingSubscriber logger = TestPubSub.Attach(hub, "broker-1", "Logger", EventType.BookingCreated);
        logger.MarkUnavailable("logger stopped");

        TestPubSub.PublishAndWait(hub, EventType.BookingCreated, 1);

        PubSubStatusReport report = hub.Status();

        Assert.Equal("AVAILABLE", report.Brokers.Single().State);
        SubscriberStatus status = report.Subscribers.Single(entry => entry.Name == "Logger");
        Assert.Equal("UNAVAILABLE", status.State);
        Assert.Equal(1, status.Pending);
        Assert.Equal(3, report.MaxDeliveryAttempts);
        Assert.Contains("queued = 1", report.Summary);
    }

    // ------------------------------------------- Требование 4 Части 5: AnalyticsService только считает

    [Fact]
    public async Task AnalyticsServiceSubscriber_HandleAsync_CountsEventsOfEveryTypeAndClient()
    {
        var analytics = new AnalyticsServiceSubscriber("AnalyticsService", 0);

        await analytics.HandleAsync(TestPubSub.Event(EventType.BookingCreated, 1, "Client 1"), "Client 1");
        await analytics.HandleAsync(TestPubSub.Event(EventType.BookingCreated, 2, "Client 2"), "Client 2");
        await analytics.HandleAsync(TestPubSub.Event(EventType.PaymentCompleted, 1, "Client 1"), "Client 1");

        Assert.Equal(2, analytics.Counters[nameof(EventType.BookingCreated)]);
        Assert.Equal(1, analytics.Counters[nameof(EventType.PaymentCompleted)]);
        Assert.Equal(2, analytics.DistinctClients);
    }

    [Fact]
    public async Task RoomServiceSubscriber_HandleAsync_KeepsRoomStateInItsOwnRegister()
    {
        var room = new RoomServiceSubscriber("RoomService", 0);

        await room.HandleAsync(BookingEvent(EventType.BookingCreated, "101"), "Client 1");
        Assert.Equal("BUSY", room.Occupancy[101]);

        await room.HandleAsync(BookingEvent(EventType.BookingCancelled, "101"), "Client 1");
        Assert.Equal("FREE", room.Occupancy[101]);
    }

    private static DomainEvent BookingEvent(EventType type, string roomId) =>
        DomainEventFactory.Create(type, 1, "Client 1", new Dictionary<string, string> { ["room"] = roomId });

    // ------------------------------------------ Связка с предметной областью: операции рождают события

    [Fact]
    public async Task DbBookingService_CreateBooking_PublishesBookingCreatedToEverySubscribedService()
    {
        (DbBookingService service, MessagingHub hub) = WithHub("create-publishes");

        Response response = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        hub.WaitForDeliveries("Tester");

        Assert.True(response.IsSuccess);
        DomainEvent published = Assert.Single(hub.PublishedEvents);
        Assert.Equal(EventType.BookingCreated, published.EventType);
        Assert.Equal(TestDatabase.ReportOf(response)!.BookingId, published.EntityId);
        Assert.True(hub.Received("Logger", published.EventId));
        Assert.True(hub.Received("NotificationService", published.EventId));
        Assert.True(hub.Received("RoomService", published.EventId));
        Assert.True(hub.Received("AnalyticsService", published.EventId));
    }

    [Fact]
    public async Task DbBookingService_PayBooking_PublishesPaymentCompleted()
    {
        (DbBookingService service, MessagingHub hub) = WithHub("pay-publishes");
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        Response paid = service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");
        hub.WaitForDeliveries("Tester");

        Assert.True(paid.IsSuccess);
        DomainEvent published = hub.PublishedEvents.Last();
        Assert.Equal(EventType.PaymentCompleted, published.EventType);
        Assert.Equal(bookingId, published.EntityId);
        Assert.True(hub.Received("NotificationService", published.EventId));
    }

    [Fact]
    public async Task DbBookingService_CancelBooking_PublishesBookingCancelled()
    {
        (DbBookingService service, MessagingHub hub) = WithHub("cancel-publishes");
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        Response cancelled = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");
        hub.WaitForDeliveries("Tester");

        Assert.True(cancelled.IsSuccess);
        DomainEvent published = hub.PublishedEvents.Last();
        Assert.Equal(EventType.BookingCancelled, published.EventType);
        Assert.True(hub.Received("Logger", published.EventId));
        Assert.False(hub.Received("NotificationService", published.EventId));
    }

    [Fact]
    public async Task DbBookingService_RejectedBooking_PublishesNothing()
    {
        (DbBookingService service, MessagingHub hub) = WithHub("rejected-publishes");

        Response missingRoom = await service.CreateBookingAsync(TestDatabase.BookingPayload(999, 1), "Client 1");

        Assert.False(missingRoom.IsSuccess);
        Assert.Empty(hub.PublishedEvents);
    }

    [Fact]
    public async Task DbBookingService_EventSubscribers_LeaveDomainDataUntouched()
    {
        (DbBookingService service, MessagingHub hub) = WithHub("subscribers-keep-data");
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;
        hub.WaitForDeliveries("Tester");

        DatabaseSnapshotReport before = Snapshot(service);
        hub.Publish(EventType.PaymentCompleted, bookingId, "Client 1",
            new Dictionary<string, string> { ["amount"] = "200.00" });
        hub.WaitForDeliveries("Tester");

        DatabaseSnapshotReport after = Snapshot(service);

        Assert.Equal(before.Bookings.Count, after.Bookings.Count);
        Assert.Equal(before.Payments.Count, after.Payments.Count);
        Assert.Equal(before.Rooms.Single(room => room.Id == 101).IsAvailable,
            after.Rooms.Single(room => room.Id == 101).IsAvailable);
    }

    [Fact]
    public async Task DbBookingService_WithoutPubSub_BooksRoomsAndExplainsMissingEventOperations()
    {
        DbBookingService service = TestDatabase.Create("hub-disabled");

        Assert.True((await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1")).IsSuccess);

        Response status = service.PubSubStatus("{}", "Client 1");

        Assert.False(status.IsSuccess);
        Assert.Contains("--pubsub on", status.Message);
    }

    /// <summary>Сервер с механизмом Pub/Sub по умолчанию: два Broker и четыре подписчика из методички.</summary>
    private static (DbBookingService Service, MessagingHub Hub) WithHub(string label)
    {
        TestPubSub.FastDelays();
        MessagingHub hub = MessagingHub.CreateDefault("Tester");

        return (TestDatabase.Create(label, hub: hub), hub);
    }

    private static DatabaseSnapshotReport Snapshot(DbBookingService service) =>
        JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "Tester").Data)!;
}
