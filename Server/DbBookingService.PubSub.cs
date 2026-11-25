using System.Text.Json;
using Server.Database;
using Server.Messaging;
using Shared.Models;
using Shared.Network;

namespace Server;

public sealed partial class DbBookingService
{
    // ------------------------------------------------------------ СОБЫТИЯ ОПЕРАЦИЙ

    private PublicationResult? PublishBookingCreated(string clientId, ReservationResult result, string instanceName)
    {
        if (_hub is null || result.Status != ReservationStatus.Success || result.Booking is null)
            return null;

        Booking booking = result.Booking;

        return Publish(clientId, EventType.BookingCreated, booking.Id, new Dictionary<string, string>
        {
            ["room"] = booking.RoomId.ToString(),
            ["guest"] = booking.GuestId.ToString(),
            ["total"] = DatabaseConfig.Number(booking.TotalAmount),
            ["status"] = booking.Status,
            ["instance"] = instanceName,
            ["transaction"] = result.Committed ? "COMMIT" : "autocommit"
        });
    }

    private PublicationResult? PublishChangeApplied(string clientId, EventType type, int bookingId,
        string instanceName, BookingStore servedFrom)
    {
        if (_hub is null)
            return null;

        var data = new Dictionary<string, string> { ["instance"] = instanceName };

        try
        {
            Booking? booking = servedFrom.ReadBookingById(bookingId);

            if (booking is not null)
            {
                data["room"] = booking.RoomId.ToString();
                data["guest"] = booking.GuestId.ToString();
                data["amount"] = DatabaseConfig.Number(booking.TotalAmount);
                data["status"] = booking.Status;
            }
        }
        catch (Exception ex)
        {
            // Событие важнее деталей: если данные дочитать не удалось, публикуются тип и идентификатор.
            data["note"] = $"booking data is unavailable: {ex.GetType().Name}";
        }

        return Publish(clientId, type, bookingId, data);
    }

    private PublicationResult? Publish(string clientId, EventType type, int entityId,
        IDictionary<string, string> data)
    {
        if (_hub is null)
            return null;

        try
        {
            return _hub.Publish(type, entityId, clientId, data);
        }
        catch (Exception ex)
        {
            // Механизм событий не может испортить результат операции с данными.
            ServerLog.Error(clientId, $"event publication error: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    private static string DescribePublication(PublicationResult? publication) =>
        publication is null
            ? string.Empty
            : publication.IsSuccess
                ? $"PUBSUB {publication.EventType} #{publication.EventId} → {string.Join(" | ", publication.AcceptedBy)}"
                : $"PUBSUB FAILED: {publication.Message}";

    // ---------------------------------------------------------- ОПЕРАЦИИ PUB/SUB

    public Response PubSubStatus(string payload, string clientId) =>
        WithHub(clientId, "PubSubStatus (subscribers and brokers)", hub =>
        {
            hub.WaitForDeliveries(clientId);
            PubSubStatusReport report = hub.Status();

            return new HubResult(true, report.Summary, report);
        });

    /// <summary>Ручная публикация: эксперимент Части 1 (Publisher → Broker → Subscriber) и проверка ошибок.</summary>
    public Response PublishEvent(string payload, string clientId)
    {
        PublishCommand? command = Parse<PublishCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "PublishEvent", "event data is not valid JSON");

        if (!EventTypes.TryParse(command.EventType, out EventType type))
        {
            ServerLog.Fail(clientId, $"PublishEvent: unknown event type '{command.EventType}'");
            return Response.Error($"Unknown event type '{command.EventType}'. Known types: " +
                                  $"{string.Join(", ", EventTypes.Names)}");
        }

        var data = new Dictionary<string, string>(command.Data);

        if (!string.IsNullOrWhiteSpace(command.RoomId))
            data["room"] = command.RoomId;

        return WithHub(clientId, $"PublishEvent ({command.EventType}, entity = {command.EntityId})", hub =>
        {
            PublicationResult publication = hub.Publish(type, command.EntityId, clientId, data);

            return new HubResult(publication.IsSuccess, publication.Message, hub.Status(), publication.EventType);
        });
    }

    public Response AddSubscriber(string payload, string clientId)
    {
        AddSubscriberCommand? command = Parse<AddSubscriberCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "AddSubscriber", "subscriber data is not valid JSON");

        return WithHub(clientId, $"AddSubscriber ({command.Name}, kind = {command.Kind})", hub =>
            hub.AddSubscriber(command.Name, command.Kind, command.Broker, command.EventTypes, clientId));
    }

    public Response SubscribeEvent(string payload, string clientId)
    {
        SubscribeCommand? command = Parse<SubscribeCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "SubscribeEvent", "subscription data is not valid JSON");

        return WithHub(clientId, $"SubscribeEvent ({command.Subscriber} → {command.EventType})", hub =>
            hub.Subscribe(command.Subscriber, command.EventType, clientId));
    }

    public Response UnsubscribeEvent(string payload, string clientId)
    {
        UnsubscribeCommand? command = Parse<UnsubscribeCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "UnsubscribeEvent", "subscription data is not valid JSON");

        return WithHub(clientId, $"UnsubscribeEvent ({command.Subscriber} from {command.EventType})", hub =>
            hub.Unsubscribe(command.Subscriber, command.EventType, clientId));
    }

    public Response FailSubscriber(string payload, string clientId)
    {
        FailSubscriberCommand? command = Parse<FailSubscriberCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "FailSubscriber", "subscriber failure data is not valid JSON");

        return WithHub(clientId, $"FailSubscriber ({command.Name}, mode = {command.Mode})", hub =>
            hub.FailSubscriber(command.Name, command.Mode, command.Attempts, command.Reason, clientId));
    }

    public Response RestoreSubscriber(string payload, string clientId)
    {
        SubscriberNameCommand? command = Parse<SubscriberNameCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "RestoreSubscriber", "subscriber name is not valid JSON");

        return WithHub(clientId, $"RestoreSubscriber ({command.Name})", hub =>
            hub.RestoreSubscriber(command.Name, clientId));
    }

    public Response FailBroker(string payload, string clientId)
    {
        BrokerNameCommand? command = Parse<BrokerNameCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "FailBroker", "broker name is not valid JSON");

        return WithHub(clientId, $"FailBroker ({command.Name})", hub =>
            hub.FailBroker(command.Name, command.Reason, clientId));
    }

    public Response RestoreBroker(string payload, string clientId)
    {
        BrokerNameCommand? command = Parse<BrokerNameCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "RestoreBroker", "broker name is not valid JSON");

        return WithHub(clientId, $"RestoreBroker ({command.Name})", hub =>
            hub.RestoreBroker(command.Name, clientId));
    }

    public Response RetryPending(string payload, string clientId) =>
        WithHub(clientId, "RetryPending (redelivery from broker queues)", hub =>
        {
            HubResult result = hub.RetryPending(clientId);

            hub.WaitForDeliveries(clientId);
            return result with { Report = hub.Status() };
        });

    /// <summary>Часть 5, Требование 6: время публикации, доставки, обработки и завершения всех подписчиков.</summary>
    public Response EventStatistics(string payload, string clientId) =>
        WithHub(clientId, "EventStatistics (delivery timings)", hub =>
        {
            hub.WaitForDeliveries(clientId);
            EventStatisticsReport report = hub.Statistics(clientId);

            return new HubResult(true, report.Summary, report);
        });

    // ------------------------------------------------------------------- СЛУЖЕБНОЕ

    /// <summary>
    /// Единая рамка для операций Pub/Sub: без --pubsub on это понятный отказ, а не NullReferenceException;
    /// результат действия возвращается как Response, а сбой слоя событий не роняет сервер.
    /// </summary>
    private Response WithHub(string clientId, string operation, Func<MessagingHub, HubResult> action)
    {
        if (_hub is null)
        {
            ServerLog.Fail(clientId, $"{operation}: the server runs without the Pub/Sub mechanism");
            return Response.Error($"{operation} is available only when the server is started with --pubsub on");
        }

        ServerLog.Start(clientId, operation);

        try
        {
            HubResult result = action(_hub);

            if (result.IsSuccess)
                ServerLog.Success(clientId, result.Message);
            else
                ServerLog.Fail(clientId, result.Message);

            return new Response
            {
                IsSuccess = result.IsSuccess,
                Message = result.Message,
                Data = result.Report is null ? string.Empty : JsonSerializer.Serialize(result.Report),
                EventTriggered = result.EventName
            };
        }
        catch (Exception ex)
        {
            ServerLog.Error(clientId, $"{operation}: {ex.GetType().Name}: {ex.Message}");
            return Response.Error($"{operation} failed: {ex.Message}");
        }
    }

    private static Response InvalidPayload(string clientId, string operation, string reason)
    {
        ServerLog.Fail(clientId, $"{operation}: invalid payload - {reason}");
        return Response.Error($"Invalid payload for {operation}: {reason}");
    }
}
