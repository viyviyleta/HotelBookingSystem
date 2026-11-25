using System.Data.Common;
using System.Text.Json;
using Server.Consensus;
using Server.Database;
using Server.Messaging;
using Server.Replication;
using Shared.Models;
using Shared.Network;

namespace Server;

public sealed partial class DbBookingService : IReplicationBookingApi, IPubSubBookingApi, IConsensusBookingApi
{
    private static readonly object SyncRoot = new();

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly BookingStore _store;

    /// <summary>Кластер «Primary + Replica». null — сервер запущен в одноэкземплярном режиме ЛР №4.</summary>
    private readonly InstanceCluster? _cluster;

    /// <summary>Механизм событий (ЛР №6). null — сервер запущен без --pubsub on, журнал остаётся как в ЛР №4/№5.</summary>
    private readonly MessagingHub? _hub;

    /// <summary>Кластер узлов (ЛР №7). null — сервер запущен без --cluster on, записи идут как в ЛР №6.</summary>
    private readonly ClusterManager? _consensus;

    public DbBookingService(BookingStore store, MessagingHub? hub = null, ClusterManager? consensus = null)
    {
        _store = store;
        _hub = hub;
        _consensus = consensus;
    }

    public DbBookingService(InstanceCluster cluster, MessagingHub? hub = null, ClusterManager? consensus = null)
    {
        _cluster = cluster;
        _store = new BookingStore(cluster.Primary);
        _hub = hub;
        _consensus = consensus;
    }

    public MessagingHub? Hub => _hub;

    public string SyncMechanism => IsSynchronized ? "lock" : "none";

    public InstanceCluster? Cluster => _cluster;

    public ClusterManager? Consensus => _consensus;

    /// <summary>Запись идёт через консенсус, только когда кластер действительно поднят.</summary>
    private bool ByConsensus => _consensus is not null && _consensus.IsRunning;

    private static bool IsSynchronized => ServerConfig.SyncMode != "none";

    // ------------------------------------------------------------- ЧТЕНИЕ ИЗ БД

    public async Task<Response> CheckAvailabilityAsync(string payload, string clientId)
    {
        ServerLog.Start(clientId, "CheckAvailability (read from database)");

        // Долгая операция намеренно вне блокировки: параллельные клиенты не должны ждать друг друга.
        await Task.Delay(ServerConfig.ReserveProcessingDelayMs);

        try
        {
            RouteDecision route = Read(clientId);

            List<Room> rooms = route.Store.GetRooms();
            List<Room> available = rooms.Where(room => room.IsAvailable).ToList();

            ServerLog.Check(clientId,
                $"table rooms: {rooms.Count} rows, is_available = TRUE → {available.Count} " +
                $"(instance = {route.InstanceName})");
            ServerLog.Success(clientId, "AvailableRooms");
            AnnounceDone(clientId, route.InstanceName, true);

            return new Response
            {
                IsSuccess = true,
                Message = $"Available rooms read from {route.InstanceName} = {available.Count}",
                Data = JsonSerializer.Serialize(available)
            };
        }
        catch (Exception ex)
        {
            return DatabaseFailure(clientId, "CheckAvailability", ex);
        }
    }

    public async Task<Response> CreateBookingAsync(string payload, string clientId)
    {
        ReservationCommand? command;
        try
        {
            command = JsonSerializer.Deserialize<ReservationCommand>(payload, JsonOptions);
        }
        catch (JsonException ex)
        {
            ServerLog.Fail(clientId, $"Invalid booking payload: {ex.Message}");
            return Response.Error("Invalid payload: booking data is not valid JSON");
        }

        if (command is null)
        {
            ServerLog.Fail(clientId, "Empty booking payload");
            return Response.Error("Invalid payload: booking data is missing");
        }

        string? validation = Validate(command);
        if (validation is not null)
        {
            ServerLog.Fail(clientId, validation);
            return Response.Error(validation);
        }

        bool useTransaction = command.UseTransaction ?? ServerConfig.UseTransactionByDefault;

        ServerLog.Start(clientId,
            $"Reserve (room {command.RoomId}, transaction = {(useTransaction ? "ON" : "OFF")}" +
            $"{(command.FailAfterFirstOperation ? ", error injected after Operation 1" : string.Empty)})");

        await Task.Delay(ServerConfig.ReserveProcessingDelayMs);

        ReservationResult result;
        ReplicationOutcome? replication;
        string instanceName;

        try
        {
            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    ServerLog.Lock(clientId);
                    RouteDecision route = Read(clientId);
                    instanceName = route.InstanceName;
                    AnnounceUpdate(clientId, route);
                    result = route.Store.Reserve(command, useTransaction, clientId);
                    replication = Replicate(route, result, command.RoomId, clientId);
                    ServerLog.Unlock(clientId);
                }
            }
            else
            {
                ServerLog.Info(clientId, "→ [NO LOCK] critical section is not protected (Race Condition experiment)");
                RouteDecision route = Read(clientId);
                instanceName = route.InstanceName;
                AnnounceUpdate(clientId, route);
                result = route.Store.Reserve(command, useTransaction, clientId, ServerConfig.RaceWindowMs);
                replication = Replicate(route, result, command.RoomId, clientId);
            }
        }
        catch (InstanceUnavailableException ex)
        {
            ServerLog.Fail(clientId, $"CreateBooking: {ex.Message}");
            AnnounceDone(clientId, "no instance", false);
            return new Response
            {
                IsSuccess = false,
                Message = $"Reservation refused: {ex.Message}",
                EventTriggered = "FailoverImpossible"
            };
        }

        PublicationResult? publication = PublishBookingCreated(clientId, result, instanceName);
        Response answer = Report(clientId, result, command.RoomId, instanceName, replication, publication);
        AnnounceDone(clientId, instanceName, answer.IsSuccess);
        return answer;
    }

    private Response Report(string clientId, ReservationResult result, int roomId, string instanceName,
        ReplicationOutcome? replication, PublicationResult? publication = null)
    {
        string outcome = result.Status switch
        {
            ReservationStatus.Success => "SUCCESS",
            ReservationStatus.RoomAlreadyReserved => "FAIL (room already reserved)",
            ReservationStatus.RoomNotFound => "FAIL (room does not exist)",
            ReservationStatus.DatabaseUnavailable => "FAIL (database unavailable)",
            _ => "FAIL (database error)"
        };

        if (result.Status == ReservationStatus.Success)
            ServerLog.Success(clientId, $"{result.Message}, transaction = {(result.Committed ? "COMMIT" : "autocommit")}");
        else
            ServerLog.Fail(clientId, result.Message);

        string replicationNote = DescribeReplication(replication);
        if (replicationNote.Length > 0)
            ServerLog.Info(clientId, $"→ [RESULT] {replicationNote}");

        string eventNote = DescribePublication(publication);
        if (eventNote.Length > 0)
            ServerLog.Info(clientId, $"→ [RESULT] {eventNote}");

        var report = new ReservationReport(
            outcome,
            result.Message,
            roomId,
            result.Booking?.Id ?? 0,
            result.UsedTransaction,
            result.Committed,
            result.RolledBack,
            result.ActiveBookingsForRoomAfter,
            result.RoomAvailableAfter,
            result.IsPartiallyApplied,
            result.DatabaseError,
            result.Steps,
            instanceName,
            replicationNote,
            eventNote);

        return new Response
        {
            IsSuccess = result.Status == ReservationStatus.Success,
            Message = result.Message,
            Data = JsonSerializer.Serialize(report),
            // Событие предметной области подменяет техническую метку только когда механизм событий включён.
            EventTriggered = result.Status switch
            {
                ReservationStatus.Success when publication is not null => publication.EventType,
                ReservationStatus.Success when replication?.IsSuccess == false => "ReplicationFailed",
                ReservationStatus.Success => result.Committed ? "BookingCommitted" : "BookingApplied",
                ReservationStatus.DatabaseUnavailable => "DatabaseUnavailable",
                _ => result.RolledBack ? "BookingRolledBack" : "BookingRejected"
            }
        };
    }

    public Response PayBooking(string payload, string clientId) => ChangeBooking(payload, clientId, "PayBooking",
        BookingStatuses.Paid, (store, id, who) => store.PayBooking(id, who), EventType.PaymentCompleted);

    public Response CancelBooking(string payload, string clientId) => ChangeBooking(payload, clientId, "CancelBooking",
        BookingStatuses.Cancelled, (store, id, who) => store.CancelBooking(id, who), EventType.BookingCancelled);

    /// <summary>Значения BookingStatus из варианта №2: их и согласует кластер.</summary>
    private static class BookingStatuses
    {
        public const string Paid = "Paid";
        public const string Cancelled = "Cancelled";
    }

    /// <summary>Результат одной записи: что произошло в базе, в реплике и какое событие опубликовано.</summary>
    private sealed record ChangeApplied(
        OperationOutcome Outcome,
        ReplicationOutcome? Replication,
        string InstanceName,
        PublicationResult? Publication,
        Response? Failure);

    private Response ChangeBooking(string payload, string clientId, string operation, string agreedStatus,
        Func<BookingStore, int, string, OperationOutcome> action, EventType domainEvent)
    {
        ServerLog.Start(clientId, ByConsensus
            ? $"{operation} (write through the cluster consensus)"
            : $"{operation} (write to database)");

        if (!TryReadBookingId(payload, out int bookingId))
        {
            ServerLog.Fail(clientId, "BookingId is missing or invalid");
            return Response.Error("BookingId is required and must be an integer");
        }

        IReadOnlyList<int> oppose = ReadOppose(payload);

        if (!ByConsensus)
            return RespondChange(clientId, operation,
                PerformChange(clientId, operation, bookingId, action, domainEvent), vote: null);

        // Требование 3 Части 7: клиент обычной операцией инициирует предложение, а запись делает
        // только лидер после согласия большинства. Запись выполняется внутри колбэка apply.
        ChangeApplied? applied = null;

        ConsensusReport vote = _consensus!.Propose(
            new Proposal("BookingStatus", agreedStatus, bookingId, oppose, clientId), clientId,
            _ =>
            {
                applied = PerformChange(clientId, operation, bookingId, action, domainEvent);

                return applied.Failure is not null
                    ? new AppliedDecision(false, applied.Failure.Message)
                    : new AppliedDecision(applied.Outcome.IsSuccess, DescribeApplied(applied),
                        applied.Publication?.EventType ?? string.Empty);
            });

        if (!vote.Achieved || applied is null)
            return RefusedByCluster(clientId, operation, bookingId, vote);

        return RespondChange(clientId, operation, applied, vote);
    }

    /// <summary>Одна запись состояния брони: блокировка, маршрут, репликация и публикация события.</summary>
    private ChangeApplied PerformChange(string clientId, string operation, int bookingId,
        Func<BookingStore, int, string, OperationOutcome> action, EventType domainEvent)
    {
        try
        {
            OperationOutcome outcome;
            ReplicationOutcome? replication;
            string instanceName = DatabaseConfig.EngineName;
            BookingStore servedFrom = _store;

            if (IsSynchronized)
            {
                lock (SyncRoot)
                {
                    ServerLog.Lock(clientId);
                    RouteDecision route = Read(clientId);
                    instanceName = route.InstanceName;
                    servedFrom = route.Store;
                    AnnounceUpdate(clientId, route);
                    outcome = action(route.Store, bookingId, clientId);
                    replication = outcome.IsSuccess ? Replicate(route, bookingId, clientId) : null;
                    ServerLog.Unlock(clientId);
                }
            }
            else
            {
                RouteDecision route = Read(clientId);
                instanceName = route.InstanceName;
                servedFrom = route.Store;
                AnnounceUpdate(clientId, route);
                outcome = action(route.Store, bookingId, clientId);
                replication = outcome.IsSuccess ? Replicate(route, bookingId, clientId) : null;
            }

            if (outcome.IsSuccess)
                ServerLog.Success(clientId, outcome.Message);
            else
                ServerLog.Fail(clientId, outcome.Message);

            PublicationResult? publication = outcome.IsSuccess
                ? PublishChangeApplied(clientId, domainEvent, bookingId, instanceName, servedFrom)
                : null;

            string eventNote = DescribePublication(publication);

            if (eventNote.Length > 0)
                ServerLog.Info(clientId, $"→ [RESULT] {eventNote}");

            AnnounceDone(clientId, instanceName, outcome.IsSuccess);

            return new ChangeApplied(outcome, replication, instanceName, publication, null);
        }
        catch (InstanceUnavailableException ex)
        {
            ServerLog.Fail(clientId, $"{operation}: {ex.Message}");
            AnnounceDone(clientId, "no instance", false);

            return new ChangeApplied(
                new OperationOutcome(false, $"{operation} refused: {ex.Message}", false, false, false,
                    ex.GetType().Name, []),
                null,
                "no instance",
                null,
                new Response
                {
                    IsSuccess = false,
                    Message = $"{operation} refused: {ex.Message}",
                    EventTriggered = "FailoverImpossible"
                });
        }
    }

    private Response RespondChange(string clientId, string operation, ChangeApplied change, ConsensusReport? vote)
    {
        if (change.Failure is not null)
            return vote is null ? change.Failure : WithConsensus(change.Failure, vote);

        OperationOutcome outcome = change.Outcome;
        string eventNote = DescribePublication(change.Publication);
        string replicationNote = DescribeReplication(change.Replication);

        Response response = new()
        {
            IsSuccess = outcome.IsSuccess,
            Message = string.Join(" | ", new[] { outcome.Message, replicationNote }
                .Where(text => text.Length > 0)),
            Data = JsonSerializer.Serialize(new ChangeReport(
                outcome.IsSuccess, outcome.Message, outcome.Committed, outcome.RolledBack, outcome.DatabaseError,
                outcome.Steps, change.InstanceName, replicationNote, eventNote)),
            EventTriggered = outcome.IsSuccess
                ? change.Publication is not null ? change.Publication.EventType
                : change.Replication?.IsSuccess == false ? "ReplicationFailed" : operation + "Committed"
                : operation + "Rejected"
        };

        return vote is null ? response : WithConsensus(response, vote);
    }

    /// <summary>Краткий итог записи: что произошло в базе, в реплике и какое событие опубликовано.</summary>
    private static string DescribeApplied(ChangeApplied change) =>
        string.Join(" | ", new[]
        {
            change.Outcome.Message,
            DescribeReplication(change.Replication),
            DescribePublication(change.Publication)
        }.Where(text => text.Length > 0));

    /// <summary>Согласие большинства добавляется в ответ отдельной строкой, чтобы его было видно в журнале клиента.</summary>
    private static Response WithConsensus(Response response, ConsensusReport vote)
    {
        response.Message = string.Join(" | ", new[] { response.Message, vote.Summary }.Where(text => text.Length > 0));
        response.EventTriggered = $"{response.EventTriggered}+{vote.Decision.Replace(" ", string.Empty)}";

        return response;
    }

    /// <summary>Большинства нет: запись не выполняется, данные остаются прежними.</summary>
    private Response RefusedByCluster(string clientId, string operation, int bookingId, ConsensusReport vote)
    {
        ServerLog.Fail(clientId, $"{operation} refused by the cluster: {vote.Summary}");

        return new Response
        {
            IsSuccess = false,
            Message = vote.Summary,
            Data = JsonSerializer.Serialize(vote),
            EventTriggered = "ConsensusNotAchieved"
        };
    }

    /// <summary>Узлы, которые должны проголосовать NO: нужно для эксперимента с разделившимися голосами.</summary>
    private static IReadOnlyList<int> ReadOppose(string payload)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(payload);

            JsonElement oppose = document.RootElement.TryGetProperty("Oppose", out JsonElement upper)
                ? upper
                : document.RootElement.TryGetProperty("oppose", out JsonElement lower) ? lower : default;

            return oppose.ValueKind == JsonValueKind.Array
                ? oppose.EnumerateArray().Select(element => element.GetInt32()).ToList()
                : [];
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            return [];
        }
    }

    // ------------------------------------------------------ СЛУЖЕБНЫЕ ОПЕРАЦИИ БД

    public Response DatabaseSnapshot(string payload, string clientId)
    {
        ServerLog.Start(clientId, "DatabaseSnapshot");

        try
        {
            RouteDecision route = Read(clientId);

            List<Room> rooms = route.Store.GetRooms();
            List<Booking> bookings = route.Store.GetBookings();
            List<Payment> payments = route.Store.GetPayments();

            int available = rooms.Count(room => room.IsAvailable);

            // Два разных нарушения инварианта: номер продан дважды и бронь при свободном номере.
            var problems = new List<string>();

            foreach (Room room in rooms)
            {
                int active = bookings.Count(b => b.RoomId == room.Id && b.Status != "Cancelled");

                if (active > 1)
                    problems.Add($"room {room.Id} sold {active} times");
                else if (active > 0 && room.IsAvailable)
                    problems.Add($"room {room.Id} has an active booking but is_available = TRUE");
            }

            string warning = problems.Count == 0
                ? "consistent"
                : "INCONSISTENT: " + string.Join("; ", problems);

            DatabaseLog.State(clientId,
                $"rooms = {rooms.Count}, available = {available}, bookings = {bookings.Count}, " +
                $"payments = {payments.Count} (instance = {route.InstanceName})");

            foreach (Room room in rooms)
                DatabaseLog.State(clientId,
                    $"room {room.Id}: type = {room.Type}, price = {DatabaseConfig.Number(room.PricePerNight)}, " +
                    $"is_available = {(room.IsAvailable ? "TRUE" : "FALSE")}" +
                    $", active bookings = {bookings.Count(b => b.RoomId == room.Id && b.Status != "Cancelled")}");

            foreach (Booking booking in bookings)
                DatabaseLog.State(clientId,
                    $"booking {booking.Id}: room = {booking.RoomId}, guest = {booking.GuestId}, " +
                    $"total = {DatabaseConfig.Number(booking.TotalAmount)}, status = {booking.Status}");

            ServerLog.Success(clientId, warning);
            AnnounceDone(clientId, route.InstanceName, problems.Count == 0);

            return new Response
            {
                IsSuccess = true,
                Message = $"DB snapshot ({route.InstanceName}): rooms = {rooms.Count} (free = {available}), " +
                          $"bookings = {bookings.Count}, payments = {payments.Count} | {warning}",
                Data = JsonSerializer.Serialize(new DatabaseSnapshotReport(
                    DatabaseConfig.EngineName, route.Store.Database.DatabaseTarget, rooms, bookings, payments,
                    available, warning, route.InstanceName))
            };
        }
        catch (Exception ex)
        {
            return DatabaseFailure(clientId, "DatabaseSnapshot", ex);
        }
    }

    public Response ResetDatabase(string payload, string clientId)
    {
        ServerLog.Start(clientId, "ResetDatabase");

        try
        {
            ReplicationOutcome? replication = null;
            string instanceName = DatabaseConfig.EngineName;

            lock (SyncRoot)
            {
                ServerLog.Lock(clientId);
                RouteDecision route = Read(clientId);
                instanceName = route.InstanceName;
                AnnounceUpdate(clientId, route);
                route.Store.Reset();

                if (_cluster is not null && route.Instance is not null && _cluster.ReplicationEnabled)
                {
                    DatabaseInstance target = _cluster.CounterpartOf(route.Instance)!;
                    replication = _cluster.Replicator.Synchronize(route.Instance, target, clientId);
                }

                ServerLog.Unlock(clientId);
            }

            ServerLog.Success(clientId, "bookings and payments cleared, all rooms is_available = TRUE");
            AnnounceDone(clientId, instanceName, replication?.IsSuccess != false);
            return new Response
            {
                IsSuccess = replication?.IsSuccess != false,
                Message = "Database state reset to seed data" +
                          (replication is null ? string.Empty : $" | {DescribeReplication(replication)}")
            };
        }
        catch (Exception ex)
        {
            return DatabaseFailure(clientId, "ResetDatabase", ex);
        }
    }

    public Response CommitFailureCheck(string payload, string clientId)
    {
        ServerLog.Start(clientId, "CommitFailureCheck (deferred foreign key)");

        try
        {
            OperationOutcome outcome;

            lock (SyncRoot)
            {
                ServerLog.Lock(clientId);
                RouteDecision route = Read(clientId);
                outcome = route.Store.SimulateCommitFailure(clientId);
                ServerLog.Unlock(clientId);
            }

            ServerLog.Fail(clientId, outcome.Message);

            return new Response
            {
                IsSuccess = false,
                Message = outcome.Message,
                Data = JsonSerializer.Serialize(outcome),
                EventTriggered = "CommitRejected"
            };
        }
        catch (Exception ex)
        {
            return DatabaseFailure(clientId, "CommitFailureCheck", ex);
        }
    }

    public Response DatabaseHealth(string payload, string clientId)
    {
        if (_cluster is null)
        {
            bool healthy = _store.Database.TryProbe(out string detail);

            ServerLog.Info(clientId, $"→ [DB HEALTH] {(healthy ? "connected" : "NOT connected")} — {detail}");

            return new Response
            {
                IsSuccess = healthy,
                Message = healthy
                    ? $"{DatabaseConfig.EngineName} is available at '{_store.Database.DatabaseTarget}' ({detail})"
                    : $"Database connection error: {detail}"
            };
        }

        List<InstanceStatus> statuses = _cluster.Status().ToList();
        bool any = statuses.Any(status => status.State == InstanceState.Available);

        foreach (InstanceStatus status in statuses)
            ReplicationLog.InstanceState(clientId, status.Role.ToString(),
                status.State == InstanceState.Available ? "AVAILABLE" : "UNAVAILABLE",
                $"{status.Schema}: {status.HealthProbe}; reason = {status.Reason}");

        return new Response
        {
            IsSuccess = any,
            Message = any
                ? $"{DatabaseConfig.EngineName} cluster is reachable: {Summarize(statuses)}, mode = {_cluster.Mode}"
                : $"Both instances are unavailable: {Summarize(statuses)}"
        };
    }

    // -------------------------------------------------- ОПЕРАЦИИ РЕПЛИКАЦИИ (ЛР №5)

    public Response InstanceStatus(string payload, string clientId)
    {
        ServerLog.Start(clientId, "InstanceStatus");

        if (_cluster is null)
            return SingleInstanceOnly(clientId, "InstanceStatus");

        List<InstanceStatus> statuses = _cluster.Status().ToList();
        ConsistencyReport? consistency = TryCompare(clientId, out string compareError);

        string summary = string.Join("; ", statuses.Select(status =>
            $"{status.Role} = {state(status.State)} ({status.Schema}, {status.HealthProbe})"));

        string serving = statuses.FirstOrDefault(status => status.State == InstanceState.Available)?.Role.ToString()
            ?? "NONE";

        ServerLog.Success(clientId, summary);

        return new Response
        {
            IsSuccess = serving != "NONE",
            Message = $"{_cluster.Mode} | serving = {serving} | {summary} | " +
                      $"{(consistency is not null ? consistency.Summary : compareError)}",
            Data = JsonSerializer.Serialize(new ClusterReport(_cluster.ReplicationEnabled,
                serving, _cluster.Mode, statuses, consistency, summary))
        };

        static string state(InstanceState value) =>
            value == InstanceState.Available ? "AVAILABLE" : "UNAVAILABLE";
    }

    public Response FailInstance(string payload, string clientId)
    {
        ServerLog.Start(clientId, "FailInstance (simulated outage)");

        if (_cluster is null)
            return SingleInstanceOnly(clientId, "FailInstance");

        if (!TryReadRole(payload, out InstanceRole role, out string? error))
        {
            ServerLog.Fail(clientId, error!);
            return Response.Error(error!);
        }

        DatabaseInstance instance = _cluster[role];
        instance.Fail($"failure injected by {clientId} at {DateTime.UtcNow:HH:mm:ss}");

        ReplicationLog.Failure(clientId,
            $"{instance.Label} → UNAVAILABLE (schema {instance.Schema}, reason = {instance.Reason})");

        ServerLog.Success(clientId, $"{instance.Label} marked UNAVAILABLE");

        return new Response
        {
            IsSuccess = true,
            Message = $"{instance.Label} is UNAVAILABLE now — {instance.Reason}",
            Data = JsonSerializer.Serialize(instance.Inspect())
        };
    }

    public Response RestoreInstance(string payload, string clientId)
    {
        ServerLog.Start(clientId, "RestoreInstance (instance brought back)");

        if (_cluster is null)
            return SingleInstanceOnly(clientId, "RestoreInstance");

        if (!TryReadRole(payload, out InstanceRole role, out string? error))
        {
            ServerLog.Fail(clientId, error!);
            return Response.Error(error!);
        }

        DatabaseInstance instance = _cluster[role];
        instance.Restore($"restored by {clientId} at {DateTime.UtcNow:HH:mm:ss}");

        (bool connected, string probe) = instance.Probe();
        ReplicationLog.Recovery(clientId,
            $"{instance.Label} → AVAILABLE (schema {instance.Schema}, {probe})" +
            (connected ? string.Empty : " — but the real connection still fails"));

        ServerLog.Success(clientId, $"{instance.Label} restored");

        return new Response
        {
            IsSuccess = connected,
            Message = $"{instance.Label} is AVAILABLE again — {probe}",
            Data = JsonSerializer.Serialize(instance.Inspect())
        };
    }

    public Response CompareReplicas(string payload, string clientId)
    {
        ServerLog.Start(clientId, "CompareReplicas (Primary vs Replica)");

        if (_cluster is null)
            return SingleInstanceOnly(clientId, "CompareReplicas");

        ConsistencyReport? report = TryCompare(clientId, out string compareError);

        if (report is null)
        {
            ServerLog.Fail(clientId, compareError);
            return new Response
            {
                IsSuccess = false,
                Message = $"Comparison failed: {compareError}",
                EventTriggered = "ComparisonFailed"
            };
        }

        return new Response
        {
            IsSuccess = true,
            Message = report.IsConsistent
                ? $"Primary = Replica | {report.Summary}"
                : $"Primary ≠ Replica | {report.Summary}",
            Data = JsonSerializer.Serialize(report),
            EventTriggered = report.IsConsistent ? "ReplicasMatch" : "ReplicasDiverged"
        };
    }

    public Response SynchronizeReplicas(string payload, string clientId)
    {
        ServerLog.Start(clientId, "SynchronizeReplicas");

        if (_cluster is null)
            return SingleInstanceOnly(clientId, "SynchronizeReplicas");

        SynchronizeCommand? command = Parse<SynchronizeCommand>(payload);
        if (command is null)
            return Response.Error("Invalid payload: synchronization parameters are not valid JSON");

        if (!Roles.TryParse(command.From, out InstanceRole source) || !Roles.TryParse(command.To, out InstanceRole target))
        {
            ServerLog.Fail(clientId, "Invalid instance role in synchronization request");
            return Response.Error("Invalid payload: From and To must be 'primary' or 'replica'");
        }

        if (source == target)
        {
            ServerLog.Fail(clientId, "Synchronization source and target are the same instance");
            return Response.Error("Synchronization is impossible: source and target must differ");
        }

        DatabaseInstance from = _cluster[source];
        DatabaseInstance to = _cluster[target];

        ReplicationOutcome outcome;

        lock (SyncRoot)
        {
            ServerLog.Lock(clientId);
            outcome = _cluster.Replicator.Synchronize(from, to, clientId);
            ServerLog.Unlock(clientId);
        }

        ConsistencyReport? consistency = TryCompare(clientId, out string compareError);
        string verdict = consistency is not null ? consistency.Summary : compareError;

        if (outcome.IsSuccess)
            ServerLog.Success(clientId, $"{outcome.Message} | {verdict}");
        else
            ServerLog.Fail(clientId, outcome.Message);

        return new Response
        {
            IsSuccess = outcome.IsSuccess,
            Message = $"{outcome.Message} | {verdict}",
            Data = JsonSerializer.Serialize(outcome),
            EventTriggered = outcome.IsSuccess
                ? consistency is not null
                    ? consistency.IsConsistent ? "SynchronizationDone" : "SynchronizedButDiverged"
                    : "SynchronizedButUnverified"
                : "SynchronizationFailed"
        };
    }

    /// <summary>
    /// Сверка копий, которая не превращает физическую потерю одной из них в «Server error»:
    /// нечитаемую копию невозможно сравнить, и это честный результат операции, а не исключение.
    /// </summary>
    private ConsistencyReport? TryCompare(string clientId, out string error)
    {
        try
        {
            error = string.Empty;
            return _cluster!.Replicator.Compare(_cluster.Primary, _cluster.Replica, clientId);
        }
        catch (Exception ex) when (ex is DatabaseConnectionException or DbException)
        {
            error = $"comparison is impossible — {ex.Message}";
            return null;
        }
    }

    // ------------------------------------------------------------------ МАРШРУТИЗАЦИЯ

    /// <summary>Как читать или писать: один экземпляр (ЛР №4) или выбранный кластером (ЛР №5).</summary>
    private RouteDecision Read(string clientId)
    {
        if (_cluster is null)
            return new RouteDecision(_store, null, DatabaseConfig.EngineName);

        DatabaseInstance instance = _cluster.Route(clientId);
        return new RouteDecision(new BookingStore(instance), instance, instance.Label);
    }

    /// <summary>Строки журнала двух копий: запись в выбранный экземпляр и итог запроса.</summary>
    private void AnnounceUpdate(string clientId, RouteDecision route)
    {
        if (_cluster is not null)
            ReplicationLog.InstanceUpdate(clientId, route.InstanceName);
    }

    private void AnnounceDone(string clientId, string instanceName, bool isSuccess)
    {
        if (_cluster is not null)
            ReplicationLog.RequestDone(clientId, instanceName, isSuccess);
    }

    private ReplicationOutcome? Replicate(RouteDecision route, ReservationResult result, int roomId, string clientId)
    {
        // Реплицируются только успешно применённые изменения: откат и отказ ничего не меняют в копии.
        if (result.Status != ReservationStatus.Success)
            return null;

        return Replicate(route, result.Booking?.Id ?? 0, new[] { roomId }, clientId);
    }

    private ReplicationOutcome? Replicate(RouteDecision route, int bookingId, string clientId) =>
        Replicate(route, bookingId, Array.Empty<int>(), clientId);

    private ReplicationOutcome? Replicate(RouteDecision route, int bookingId, IReadOnlyList<int> roomIds,
        string clientId)
    {
        if (_cluster is null || route.Instance is null || !_cluster.ReplicationEnabled)
            return null;

        DatabaseInstance target = _cluster.CounterpartOf(route.Instance)!;
        IReadOnlyList<int> bookings = bookingId > 0 ? new[] { bookingId } : Array.Empty<int>();

        return _cluster.Replicator.ReplicateChange(route.Instance, target, bookings, roomIds, clientId);
    }

    private static string DescribeReplication(ReplicationOutcome? replication)
    {
        if (replication is null)
            return string.Empty;

        return replication.IsSuccess
            ? $"REPLICATION {replication.Source} → {replication.Target}: OK " +
              $"(rows: bookings = {replication.BookingRows}, payments = {replication.PaymentRows}, rooms = {replication.RoomRows})"
            : $"REPLICATION FAILED: {replication.Error ?? replication.Message}";
    }

    private static bool TryReadRole(string payload, out InstanceRole role, out string? error)
    {
        role = InstanceRole.Primary;
        error = null;

        InstanceCommand? command = Parse<InstanceCommand>(payload);
        if (command is null)
        {
            error = "Invalid payload: instance role is missing";
            return false;
        }

        if (Roles.TryParse(command.Role, out role))
            return true;

        error = $"Invalid payload: unknown instance role '{command.Role}' (expected 'primary' or 'replica')";
        return false;
    }

    private static T? Parse<T>(string payload) where T : class
    {
        try
        {
            return string.IsNullOrWhiteSpace(payload) || payload == "{}"
                ? Activator.CreateInstance<T>()
                : JsonSerializer.Deserialize<T>(payload, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Summarize(IReadOnlyList<InstanceStatus> statuses) =>
        string.Join(", ", statuses.Select(status => $"{status.Role} = {status.State}"));

    // ------------------------------------------------------------------ ПРОВЕРКИ

    private static string? Validate(ReservationCommand command)
    {
        if (command.RoomId <= 0)
            return "Invalid data: RoomId is required";

        if (command.GuestId <= 0)
            return "Invalid data: GuestId is required";

        if (command.CheckInDate == default || command.CheckOutDate == default)
            return "Invalid data: check-in and check-out dates are required";

        if (command.CheckOutDate <= command.CheckInDate)
            return "Check-out date must be after check-in date";

        if (command.CheckInDate.Date < DateTime.Today)
            return "Check-in date cannot be in the past";

        return null;
    }

    private static bool TryReadBookingId(string payload, out int bookingId)
    {
        bookingId = 0;

        try
        {
            using var document = JsonDocument.Parse(payload);
            return document.RootElement.TryGetProperty("BookingId", out var element) && element.TryGetInt32(out bookingId);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static Response SingleInstanceOnly(string clientId, string operation)
    {
        ServerLog.Fail(clientId, $"{operation}: the server runs with a single data instance");
        return Response.Error($"{operation} is available only when the server is started with --replication on");
    }

    private static Response DatabaseFailure(string clientId, string operation, Exception ex)
    {
        string reason = ex is DatabaseConnectionException or InstanceUnavailableException or DbException
            ? $"database error — {ex.Message}"
            : $"{ex.GetType().Name}: {ex.Message}";

        ServerLog.Fail(clientId, $"{operation}: {reason}");
        return Response.Error($"{operation} failed: {reason}");
    }

    private sealed record RouteDecision(BookingStore Store, DatabaseInstance? Instance, string InstanceName);
}

/// <summary>Ответ составной операции: то, что клиент печатает в таблицу эксперимента.</summary>
public sealed record ReservationReport(
    string Outcome,
    string Message,
    int RoomId,
    int BookingId,
    bool UsedTransaction,
    bool Committed,
    bool RolledBack,
    int ActiveBookingsForRoom,
    bool RoomAvailableAfter,
    bool PartialChange,
    string? DatabaseError,
    IReadOnlyList<string> Steps,
    string ServedBy = "database",
    string Replication = "",
    string Event = "");

public sealed record DatabaseSnapshotReport(
    string Engine,
    string Target,
    List<Room> Rooms,
    List<Booking> Bookings,
    List<Payment> Payments,
    int AvailableRooms,
    string ConsistencyWarning,
    string ServedBy = "database");

/// <summary>Ответ одиночной операции изменения (PayBooking, CancelBooking) вместе с событиями репликации.</summary>
public sealed record ChangeReport(
    bool IsSuccess,
    string Message,
    bool Committed,
    bool RolledBack,
    string? DatabaseError,
    IReadOnlyList<string> Steps,
    string ServedBy,
    string Replication,
    string Event = "");
