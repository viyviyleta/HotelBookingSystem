using System.Text.Json;
using Shared.Network;

namespace Client;


public sealed partial class ScenarioRunner
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public bool EnsureDatabaseMode()
    {
        Response health = _client.SendRequest(RequestBuilder.BuildDatabaseHealth());

        if (!health.IsSuccess)
        {
            ClientLog.Line($"[DB] {health.Message}");
            ClientLog.Line("[DB] start the server with: --storage postgres");
            return false;
        }

        ClientLog.Line($"[DB] {health.Message}");
        return true;
    }

    public void ResetDatabase()
    {
        Response response = _client.SendRequest(RequestBuilder.BuildResetDatabase());
        ClientLog.Line($"[DB] reset → {response.Message}");
    }

    // ------------------------------------------------ Часть 2: конкурентный доступ

    public void RunDatabaseRace(int roomId, int clientsCount)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR4 PART 2: CONCURRENT ACCESS TO A DATABASE ROW, room {roomId}, clients = {clientsCount} ===");

        PrintRoomState("before", roomId);

        // Внешний ключ bookings.guest_id允许 только реально существующих гостей (id 1..3).
        List<(string Who, TimedResponse Timed)> results = ConcurrentReserve(roomId, clientsCount,
            number => RequestBuilder.BuildCreateBooking(
                roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3),
                guestId: 1 + (number - 1) % 3, useTransaction: null, failAfterFirstOperation: false));

        foreach ((string who, TimedResponse timed) in results)
        {
            ReservationReport? report = Parse(timed.Response);
            string outcome = report?.Outcome ?? (timed.Response.IsSuccess ? "SUCCESS" : "FAIL");

            ClientLog.Line(
                $"{who} → sent {T(timed.SentAt)}, received {T(timed.ReceivedAt)}, booking #{report?.BookingId ?? 0} → " +
                $"{outcome}: {timed.Response.Message}");
        }

        PrintRoomState("after", roomId);

        Response snapshot = _client.SendRequest(RequestBuilder.BuildDatabaseSnapshot());
        ClientLog.Line($"[DB CONSISTENCY] {snapshot.Message}");
    }

    // --------------------------------------------- Часть 3: транзакция и её отсутствие

    public void RunTransactionExperiments(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR4 PART 3: COMPOSITE OPERATION WITH AND WITHOUT A TRANSACTION ===");
        ClientLog.Line($"Composite operation: Operation 1 = INSERT bookings, Operation 2 = UPDATE rooms.is_available");

        var experiments = new (int Number, bool UseTransaction, bool InjectError)[]
        {
            (1, false, false),
            (2, false, true),
            (3, true, false),
            (4, true, true)
        };

        foreach ((int number, bool useTransaction, bool injectError) in experiments)
        {
            ResetDatabase();
            RunSingleTransactionExperiment(roomId, number, useTransaction, injectError);
        }

        RunCommitFailureExperiment(roomId);
    }

    private void RunSingleTransactionExperiment(int roomId, int number, bool useTransaction, bool injectError)
    {
        ClientLog.Line("");
        ClientLog.Line($"--- Experiment {number}: transaction = {(useTransaction ? "YES" : "NO")}, " +
                       $"error between operations = {(injectError ? "YES" : "NO")} ---");

        PrintRoomState("before", roomId);

        Response response = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3),
            guestId: 1, useTransaction: useTransaction, failAfterFirstOperation: injectError));

        ReservationReport? report = Parse(response);

        if (report is null)
        {
            ClientLog.Line($"[!] unexpected answer: {response.Message}");
            return;
        }

        bool operation1 = report.Steps.Any(step => step.StartsWith("INSERT bookings", StringComparison.OrdinalIgnoreCase));
        bool operation2 = report.Steps.Any(step => step.StartsWith("UPDATE rooms", StringComparison.OrdinalIgnoreCase));

        ClientLog.Line($"Operation 1 (INSERT bookings) → {(operation1 ? "executed" : "NOT executed")}" +
                       (operation1 ? $", booking id = {report.BookingId}" : string.Empty));
        ClientLog.Line($"Operation 2 (UPDATE rooms)    → {(operation2 ? "executed" : "NOT executed")}");
        ClientLog.Line($"Error                         → {(injectError ? "yes (injected between the operations)" : "no")}");
        ClientLog.Line($"Transaction                   → {(report.UsedTransaction ? (report.Committed ? "COMMIT" : "ROLLBACK") : "not used (autocommit)")}");

        if (report.DatabaseError is not null)
            ClientLog.Line($"Database error                → {report.DatabaseError}");

        PrintRoomState("after", roomId);

        string verdict = report.PartialChange
            ? "PARTIAL CHANGE: the booking row stayed in the database while the room is free again — inconsistent"
            : report.Outcome == "SUCCESS"
                ? "changes saved, no leftovers"
                : report.RolledBack
                    ? "nothing left in the database — the transaction was cancelled"
                    : "rejected before writing, database unchanged";

        ClientLog.Line($"Result: {report.Outcome} | {verdict}");
    }

    private void RunCommitFailureExperiment(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("--- Experiment 5: COMMIT is impossible (deferred foreign key) ---");

        PrintRoomState("before", roomId);

        Response response = _client.SendRequest(RequestBuilder.BuildCommitFailureCheck());
        ClientLog.Line($"Answer: {response.Message}");
        ClientLog.Line($"Event:  {response.EventTriggered}");

        PrintRoomState("after", roomId);
    }

    // -------------------------------------------------- Требование 8: обработка ошибок

    public void RunDatabaseErrorChecks(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR4 ERROR HANDLING CHECKS ===");

        ResetDatabase();

        ClientLog.Line($"Unknown operation        → {_client.SendRequest(new Request { Operation = "TeleportRoom", Payload = "{}" }).Message}");
        ClientLog.Line($"Malformed request        → {_client.SendRequest(RequestBuilder.BuildInvalidRequest()).Message}");
        ClientLog.Line($"Missing room (resource)  → {_client.SendRequest(RequestBuilder.BuildCreateBooking(999, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 1)).Message}");
        ClientLog.Line($"Reserve existing room    → {_client.SendRequest(RequestBuilder.BuildCreateBooking(roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 1)).Message}");

        ClientLog.Line($"Reserved room again      → {_client.SendRequest(RequestBuilder.BuildCreateBooking(roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 2)).Message}");
        ClientLog.Line($"Missing record to change → {_client.SendRequest(RequestBuilder.BuildPayBooking(4242)).Message}");
        ClientLog.Line($"DB constraint (FK)       → {_client.SendRequest(RequestBuilder.BuildCreateBooking(102, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), guestId: 99999)).Message}");
        ClientLog.Line($"Error inside transaction → {OutcomeOf(_client.SendRequest(RequestBuilder.BuildCreateBooking(102, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), 1, useTransaction: true, failAfterFirstOperation: true)))}");
        ClientLog.Line($"COMMIT impossible        → {_client.SendRequest(RequestBuilder.BuildCommitFailureCheck()).Message}");
        ClientLog.Line($"Invalid dates            → {_client.SendRequest(new Request { Operation = "CreateBooking", Payload = """{"RoomId":101,"GuestId":1,"CheckInDate":"2030-05-10","CheckOutDate":"2030-05-01"}""" }).Message}");
        ClientLog.Line($"Missing server           → {new NetworkClient("127.0.0.1", 5999).SendRequest(RequestBuilder.BuildDatabaseSnapshot()).Message}");

        PrintRoomState("final", roomId);
    }

    public void RunDatabaseOutageChecks(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR4: DATABASE CONNECTION FAILURE ===");

        ClientLog.Line($"DatabaseHealth     → {_client.SendRequest(RequestBuilder.BuildDatabaseHealth()).Message}");
        ClientLog.Line($"CheckAvailability  → {_client.SendRequest(RequestBuilder.BuildCheckAvailability()).Message}");
        ClientLog.Line($"CreateBooking      → {_client.SendRequest(RequestBuilder.BuildCreateBooking(roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), 1)).Message}");
        ClientLog.Line($"DatabaseSnapshot   → {_client.SendRequest(RequestBuilder.BuildDatabaseSnapshot()).Message}");

        Response time = _client.SendRequest(RequestBuilder.BuildGetServerTime(0));
        ClientLog.Line($"Server is still alive (operation without DB) → IsSuccess = {time.IsSuccess}");
    }

    // ---------------------------------------------------------------------- УТИЛИТЫ

    private static string OutcomeOf(Response response) =>
        Parse(response)?.Outcome ?? response.Message;

    private static ReservationReport? Parse(Response response) =>
        string.IsNullOrEmpty(response.Data)
            ? null
            : JsonSerializer.Deserialize<ReservationReport>(response.Data, Json);

    private void PrintRoomState(string label, int roomId)
    {
        Response response = _client.SendRequest(RequestBuilder.BuildDatabaseSnapshot());
        DatabaseSnapshotReport? snapshot = string.IsNullOrEmpty(response.Data)
            ? null
            : JsonSerializer.Deserialize<DatabaseSnapshotReport>(response.Data, Json);

        RoomRow? room = snapshot?.Rooms.Find(candidate => candidate.Id == roomId);

        if (snapshot is null || room is null)
        {
            ClientLog.Line($"[DB {label}] cannot read the state of room {roomId}: {response.Message}");
            return;
        }

        int active = snapshot.Bookings.Count(booking => booking.RoomId == roomId && booking.Status != "Cancelled");

        ClientLog.Line($"[DB {label}] room {roomId}: is_available = {(room.IsAvailable ? "TRUE" : "FALSE")}, " +
                       $"active bookings = {active}, total bookings = {snapshot.Bookings.Count}, " +
                       $"payments = {snapshot.Payments.Count} | {snapshot.ConsistencyWarning}");
    }
}
