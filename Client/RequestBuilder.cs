using System.Globalization;
using System.Text.Json;
using Shared.Network;

namespace Client;

public static class RequestBuilder
{
    private static int _currentGuestId = 0;

    private static int ReadInt(string prompt)
    {
        while (true)
        {
            Console.Write(prompt);
            string? input = Console.ReadLine();

            if (int.TryParse(input, out int result) && result > 0)
            {
                return result;
            }

            Console.WriteLine("[!] Error: Please enter a positive integer!");
        }
    }

    private static DateTime ReadDate(string prompt)
    {
        while (true)
        {
            Console.Write(prompt);
            string? input = Console.ReadLine();

            if (DateTime.TryParseExact(input, new[] { "yyyy-MM-dd", "dd.MM.yyyy" },
                CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate))
            {
                return parsedDate;
            }

            Console.WriteLine("[!] Error: Please enter a valid date format YYYY-MM-DD (e.g., 2026-10-15) or DD.MM.YYYY!");
        }
    }

    public static Request BuildCheckAvailability()
    {
        return new Request { Operation = "CheckAvailability", Payload = "{}" };
    }

    public static Request BuildCreateBooking()
    {
        int roomId = ReadInt("Enter Room ID (e.g., 101, 102, 201): ");

        DateTime checkIn;
        while (true)
        {
            checkIn = ReadDate("Enter Check-In Date (YYYY-MM-DD ): ");
            if (checkIn.Date >= DateTime.Today)
            {
                break;
            }
            Console.WriteLine("[!] Error: Check-in date cannot be in the past!");
        }

        DateTime checkOut;
        while (true)
        {
            checkOut = ReadDate("Enter Check-Out Date (YYYY-MM-DD ): ");
            if (checkOut > checkIn)
            {
                break;
            }
            Console.WriteLine("[!] Error: Check-out date must be strictly after check-in date!");
        }

        int guestId = Interlocked.Add(ref _currentGuestId, 1);

        return BuildCreateBooking(roomId, checkIn, checkOut, guestId);
    }


    public static Request BuildCreateBooking(int roomId, DateTime checkIn, DateTime checkOut, int guestId) =>
        BuildCreateBooking(roomId, checkIn, checkOut, guestId, useTransaction: null, failAfterFirstOperation: false);

    public static Request BuildCreateBooking(int roomId, DateTime checkIn, DateTime checkOut, int guestId,
        bool? useTransaction, bool failAfterFirstOperation)
    {
        string payload = JsonSerializer.Serialize(new
        {
            GuestId = guestId,
            RoomId = roomId,
            CheckInDate = checkIn,
            CheckOutDate = checkOut,
            UseTransaction = useTransaction,
            FailAfterFirstOperation = failAfterFirstOperation
        });

        return new Request { Operation = "CreateBooking", Payload = payload };
    }

    public static Request BuildPayBooking()
    {
        int payId = ReadInt("Enter Booking ID to pay: ");

        return new Request
        {
            Operation = "PayBooking",
            Payload = JsonSerializer.Serialize(new { BookingId = payId })
        };
    }

    public static Request BuildCancelBooking()
    {
        int cancelId = ReadInt("Enter Booking ID to cancel: ");

        return new Request
        {
            Operation = "CancelBooking",
            Payload = JsonSerializer.Serialize(new { BookingId = cancelId })
        };
    }

    public static Request BuildGetServerTime(int delayMs)
    {
        return new Request
        {
            Operation = "GetServerTime",
            Payload = JsonSerializer.Serialize(new { DelayMs = delayMs })
        };
    }

    public static Request BuildInvalidRequest()
    {
        return new Request
        {
            Operation = "CreateBooking",
            Payload = "{ not a json }"
        };
    }

    // ------------------------------ Операции базы данных (ЛР №4)

    public static Request BuildDatabaseSnapshot() => new() { Operation = "DatabaseSnapshot", Payload = "{}" };

    public static Request BuildResetDatabase() => new() { Operation = "ResetDatabase", Payload = "{}" };

    public static Request BuildDatabaseHealth() => new() { Operation = "DatabaseHealth", Payload = "{}" };

    public static Request BuildCommitFailureCheck() => new() { Operation = "CommitFailureCheck", Payload = "{}" };

    public static Request BuildPayBooking(int bookingId, IEnumerable<int>? oppose = null) => new()
    {
        Operation = "PayBooking",
        Payload = JsonSerializer.Serialize(new { BookingId = bookingId, Oppose = oppose?.ToList() ?? new List<int>() })
    };

    public static Request BuildCancelBooking(int bookingId, IEnumerable<int>? oppose = null) => new()
    {
        Operation = "CancelBooking",
        Payload = JsonSerializer.Serialize(new { BookingId = bookingId, Oppose = oppose?.ToList() ?? new List<int>() })
    };

    // ------------------------------ Две копии данных и отказоустойчивость (ЛР №5)

    public static Request BuildInstanceStatus() => new() { Operation = "InstanceStatus", Payload = "{}" };

    public static Request BuildCompareReplicas() => new() { Operation = "CompareReplicas", Payload = "{}" };

    public static Request BuildFailInstance(string role) => InstanceRequest("FailInstance", role);

    public static Request BuildRestoreInstance(string role) => InstanceRequest("RestoreInstance", role);

    public static Request BuildSynchronizeReplicas(string from, string to) => new()
    {
        Operation = "SynchronizeReplicas",
        Payload = JsonSerializer.Serialize(new { From = from, To = to })
    };

    private static Request InstanceRequest(string operation, string role) => new()
    {
        Operation = operation,
        Payload = JsonSerializer.Serialize(new { Role = role })
    };

    // ------------------------------ Pub/Sub: Publisher, Broker, Subscriber (ЛР №6)

    public static Request BuildPubSubStatus() => new() { Operation = "PubSubStatus", Payload = "{}" };

    public static Request BuildEventStatistics() => new() { Operation = "EventStatistics", Payload = "{}" };

    public static Request BuildRetryPending() => new() { Operation = "RetryPending", Payload = "{}" };

    /// <summary>Ручная публикация события: эксперименты Части 1 и проверки обработки ошибок.</summary>
    public static Request BuildPublishEvent(string eventType, int entityId, string? roomId = null,
        IReadOnlyDictionary<string, string>? data = null) => new()
    {
        Operation = "PublishEvent",
        Payload = JsonSerializer.Serialize(new
        {
            EventType = eventType,
            EntityId = entityId,
            RoomId = roomId,
            Data = data ?? new Dictionary<string, string>()
        })
    };

    public static Request BuildAddSubscriber(string name, string kind, string broker = "",
        IEnumerable<string>? eventTypes = null) => new()
    {
        Operation = "AddSubscriber",
        Payload = JsonSerializer.Serialize(new
        {
            Name = name,
            Kind = kind,
            Broker = broker,
            EventTypes = eventTypes?.ToList() ?? new List<string>()
        })
    };

    public static Request BuildSubscribeEvent(string subscriber, string eventType) =>
        SubscriptionRequest("SubscribeEvent", subscriber, eventType);

    public static Request BuildUnsubscribeEvent(string subscriber, string eventType) =>
        SubscriptionRequest("UnsubscribeEvent", subscriber, eventType);

    public static Request BuildFailSubscriber(string name, string mode = "unavailable", int attempts = 1,
        string reason = "") => new()
    {
        Operation = "FailSubscriber",
        Payload = JsonSerializer.Serialize(new { Name = name, Mode = mode, Attempts = attempts, Reason = reason })
    };

    public static Request BuildRestoreSubscriber(string name) => SubscriberRequest("RestoreSubscriber", name);

    public static Request BuildFailBroker(string name, string reason = "") => new()
    {
        Operation = "FailBroker",
        Payload = JsonSerializer.Serialize(new { Name = name, Reason = reason })
    };

    public static Request BuildRestoreBroker(string name) => BrokerRequest("RestoreBroker", name);

    // --------------------------------------- Кластер узлов, выбор лидера и консенсус (ЛР №7)

    public static Request BuildClusterStatus() => new() { Operation = "ClusterStatus", Payload = "{}" };

    public static Request BuildClusterStatistics(int lines = 20) => new()
    {
        Operation = "ClusterStatistics",
        Payload = JsonSerializer.Serialize(new { Lines = lines })
    };

    public static Request BuildClusterJournal(int lines = 14) => new()
    {
        Operation = "ClusterJournal",
        Payload = JsonSerializer.Serialize(new { Lines = lines })
    };

    public static Request BuildStartElection(string reason) => new()
    {
        Operation = "StartElection",
        Payload = JsonSerializer.Serialize(new { Reason = reason })
    };

    public static Request BuildHeartbeatProbe() => new() { Operation = "HeartbeatProbe", Payload = "{}" };

    public static Request BuildFailNode(int node, string mode = "state", string reason = "") => new()
    {
        Operation = "FailNode",
        Payload = JsonSerializer.Serialize(new { Node = node, Mode = mode, Reason = reason })
    };

    public static Request BuildRestoreNode(int node, string reason = "") => new()
    {
        Operation = "RestoreNode",
        Payload = JsonSerializer.Serialize(new { Node = node, Reason = reason })
    };

    public static Request BuildProposeChange(string subject, string value, int entityId = 0,
        IEnumerable<int>? oppose = null) => new()
    {
        Operation = "ProposeChange",
        Payload = JsonSerializer.Serialize(new
        {
            Subject = subject,
            Value = value,
            EntityId = entityId,
            Oppose = oppose?.ToList() ?? new List<int>()
        })
    };

    public static Request BuildSendNodeMessage(int from, int to, string type = "HEARTBEAT") => new()
    {
        Operation = "SendNodeMessage",
        Payload = JsonSerializer.Serialize(new { From = from, To = to, Type = type })
    };

    public static Request BuildBroadcastNodes(string type = "STATUS") => new()
    {
        Operation = "BroadcastNodes",
        Payload = JsonSerializer.Serialize(new { Type = type })
    };

    private static Request SubscriptionRequest(string operation, string subscriber, string eventType) => new()
    {
        Operation = operation,
        Payload = JsonSerializer.Serialize(new { Subscriber = subscriber, EventType = eventType })
    };

    private static Request SubscriberRequest(string operation, string name) => new()
    {
        Operation = operation,
        Payload = JsonSerializer.Serialize(new { Name = name })
    };

    private static Request BrokerRequest(string operation, string name) => new()
    {
        Operation = operation,
        Payload = JsonSerializer.Serialize(new { Name = name })
    };
}
