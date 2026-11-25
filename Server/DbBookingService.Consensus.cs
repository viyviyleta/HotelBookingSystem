using System.Text.Json;
using Server.Consensus;
using Server.Database;
using Server.Messaging;
using Shared.Network;

namespace Server;


public sealed partial class DbBookingService
{
    private static readonly string[] BookingStatusValues = ["Created", "Paid", "Cancelled"];

    // ------------------------------------------------------------------ СОСТОЯНИЕ И ВЫБОРЫ

    public Response ClusterStatus(string payload, string clientId) =>
        WithCluster(clientId, "ClusterStatus (nodes, roles and leader)", cluster =>
            new ClusterResult(true, cluster.Status().Summary, cluster.Status()));
    public Response StartElection(string payload, string clientId)
    {
        ElectionCommand? command = Parse<ElectionCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "StartElection", "election data is not valid JSON");

        return WithCluster(clientId, $"StartElection (reason = {command.Reason})", cluster =>
        {
            ElectionReport report = cluster.Elect(command.Reason, clientId);

            return new ClusterResult(report.LeaderId > 0, report.Summary, report);
        });
    }

    public Response HeartbeatProbe(string payload, string clientId) =>
        WithCluster(clientId, "HeartbeatProbe (HEARTBEAT between nodes)", cluster =>
        {
            HeartbeatReport report = cluster.Probe(clientId);

            // Сам опрос выполнен успешно: отказ узла - это результат проверки, а не ошибка операции.
            return new ClusterResult(true, report.Summary, report);
        });

    // ------------------------------------------------------------------ ОТКАЗ И ВОССТАНОВЛЕНИЕ

    public Response FailNode(string payload, string clientId)
    {
        FailNodeCommand? command = Parse<FailNodeCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "FailNode", "node failure data is not valid JSON");

        return WithCluster(clientId, $"FailNode (Node {command.Node}, mode = {command.Mode})", cluster =>
            cluster.FailNode(command.Node, command.Mode, command.Reason, clientId));
    }

    public Response RestoreNode(string payload, string clientId)
    {
        RestoreNodeCommand? command = Parse<RestoreNodeCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "RestoreNode", "node name is not valid JSON");

        return WithCluster(clientId, $"RestoreNode (Node {command.Node})", cluster =>
            cluster.RestoreNode(command.Node, command.Reason, clientId));
    }

    // ------------------------------------------------------------------ ГОЛОСОВАНИЕ

    public Response ProposeChange(string payload, string clientId)
    {
        ProposeCommand? command = Parse<ProposeCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "ProposeChange", "proposal data is not valid JSON");

        string subject = command.Subject.Trim();

        if (subject.Equals("BookingStatus", StringComparison.OrdinalIgnoreCase))
        {
            if (!BookingStatusValues.Contains(command.Value, StringComparer.OrdinalIgnoreCase))
                return UnknownStatus(clientId, command.Value);

            if (command.EntityId <= 0)
                return Response.Error("ProposeChange for BookingStatus needs a positive EntityId: " +
                                      "that is the booking whose status the cluster agrees on");

            EventType domainEvent = command.Value.Equals(BookingStatusValues[1], StringComparison.OrdinalIgnoreCase)
                ? EventType.PaymentCompleted
                : EventType.BookingCancelled;

            Func<BookingStore, int, string, OperationOutcome> action =
                domainEvent == EventType.PaymentCompleted
                    ? (store, id, who) => store.PayBooking(id, who)
                    : (store, id, who) => store.CancelBooking(id, who);

            return Propose(clientId, "BookingStatus", command.Value, command.EntityId, command.Oppose, domainEvent, action);
        }

        // Значение вне предметной области: кластер его согласует, но в базе менять нечего.
        return WithCluster(clientId, $"ProposeChange ({subject} = {command.Value})", cluster =>
        {
            ConsensusReport report = cluster.Propose(
                new Proposal(subject, command.Value, 0, command.Oppose, clientId), clientId);

            return new ClusterResult(report.Achieved, WithNote(report), report);
        });
    }

    private Response Propose(string clientId, string subject, string value, int bookingId,
        IReadOnlyList<int> oppose, EventType domainEvent, Func<BookingStore, int, string, OperationOutcome> action)
    {
        return WithCluster(clientId, $"ProposeChange ({subject}={value}, booking {bookingId})", cluster =>
        {
            ChangeApplied? applied = null;

            ConsensusReport report = cluster.Propose(new Proposal(subject, value, bookingId, oppose, clientId),
                clientId,
                _ =>
                {
                    applied = PerformChange(clientId, domainEvent == EventType.PaymentCompleted
                        ? "PayBooking"
                        : "CancelBooking", bookingId, action, domainEvent);

                    return applied.Failure is not null
                        ? new AppliedDecision(false, applied.Failure.Message)
                        : new AppliedDecision(applied.Outcome.IsSuccess, DescribeApplied(applied),
                            applied.Publication?.EventType ?? string.Empty);
                });

            if (!report.Achieved || applied is null)
                return new ClusterResult(false, WithNote(report), report);

            return new ClusterResult(applied.Outcome.IsSuccess, WithNote(report), report);
        });
    }

    private static string WithNote(ConsensusReport report) =>
        report.AppliedNote.Length > 0 ? $"{report.Summary} | {report.AppliedNote}" : report.Summary;

    private Response UnknownStatus(string clientId, string value)
    {
        ServerLog.Fail(clientId, $"ProposeChange: unknown BookingStatus '{value}'");

        return Response.Error($"Unknown BookingStatus '{value}'. The schema allows: " +
                              $"{string.Join(", ", BookingStatusValues)}");
    }

    // ------------------------------------------------------------------ СООБЩЕНИЯ И ЖУРНАЛ

    public Response SendNodeMessage(string payload, string clientId)
    {
        NodeMessageCommand? command = Parse<NodeMessageCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "SendNodeMessage", "message data is not valid JSON");

        return WithCluster(clientId, $"SendNodeMessage (Node {command.From} → Node {command.To}, {command.Type})",
            cluster => cluster.SendMessage(command.From, command.To, command.Type, clientId));
    }

    public Response BroadcastNodes(string payload, string clientId)
    {
        BroadcastCommand? command = Parse<BroadcastCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "BroadcastNodes", "broadcast data is not valid JSON");

        return WithCluster(clientId, $"BroadcastNodes ({command.Type} to every node at once)",
            cluster => cluster.Broadcast(command.Type, clientId));
    }

    public Response ClusterJournal(string payload, string clientId)
    {
        JournalCommand? command = Parse<JournalCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "ClusterJournal", "journal request is not valid JSON");

        return WithCluster(clientId, $"ClusterJournal (last {command.Lines} lines)", cluster =>
        {
            IReadOnlyList<JournalEntry> lines = cluster.RecentJournal(command.Lines);

            return new ClusterResult(true,
                $"journal: {lines.Count} line(s), last = " +
                $"{(lines.Count > 0 ? lines[lines.Count - 1].ToString() : "empty")}",
                new ClusterJournalReport(lines.Count, cluster.Journal.Count, lines));
        });
    }

    public Response ClusterStatistics(string payload, string clientId)
    {
        JournalCommand? command = Parse<JournalCommand>(payload);

        if (command is null)
            return InvalidPayload(clientId, "ClusterStatistics", "statistics request is not valid JSON");

        return WithCluster(clientId, "ClusterStatistics (votes, heartbeats and timeouts)", cluster =>
        {
            ClusterStatisticsReport report = cluster.Statistics(command.Lines);

            return new ClusterResult(true, report.Summary, report);
        });
    }

    // ------------------------------------------------------------------ СЛУЖЕБНОЕ

    private Response WithCluster(string clientId, string operation, Func<ClusterManager, ClusterResult> action)
    {
        if (!ByConsensus)
        {
            ServerLog.Fail(clientId, $"{operation}: the server runs without a node cluster");
            return Response.Error($"{operation} is available only when the server is started with --cluster on");
        }

        ServerLog.Start(clientId, operation);

        try
        {
            ClusterResult result = action(_consensus!);

            if (result.IsSuccess)
                ServerLog.Success(clientId, result.Message);
            else
                ServerLog.Fail(clientId, result.Message);

            return new Response
            {
                IsSuccess = result.IsSuccess,
                Message = result.Message,
                Data = result.Report is null ? string.Empty : JsonSerializer.Serialize(result.Report)
            };
        }
        catch (Exception ex)
        {
            ServerLog.Error(clientId, $"{operation}: {ex.GetType().Name}: {ex.Message}");
            return Response.Error($"{operation} failed: {ex.Message}");
        }
    }
}

/// <summary>Отчёт журнала кластера для таблицы клиента.</summary>
public sealed record ClusterJournalReport(int Returned, int Total, IReadOnlyList<JournalEntry> Lines);
