using System.Text.Json;
using Shared.Network;

namespace Server;

/// <summary>
/// Разбор запроса и диспетчеризация операций. Один экземпляр обрабатывает
/// запросы всех клиентов, поэтому бизнес-логика (и её критические секции)
/// должна быть потокобезопасной.
/// </summary>
public class RequestHandler
{
    private readonly IBookingApi _logic;

    public RequestHandler() : this(new LockBusinessLogic())
    {
    }

    public RequestHandler(IBookingApi logic)
    {
        _logic = logic;
    }

    public Task<string> HandleRequestAsync(string jsonRequest, string clientId) =>
        HandleRequestAsync(jsonRequest, new ConnectionId(clientId));

    public async Task<string> HandleRequestAsync(string jsonRequest, ConnectionId clientId)
    {
        Request? request;
        try
        {
            request = JsonSerializer.Deserialize<Request>(jsonRequest);
        }
        catch (JsonException ex)
        {
            ServerLog.Fail(clientId.Name, $"Invalid request format: {ex.Message}");
            return Serialize(Response.Error("Invalid request format: request is not valid JSON"));
        }

        if (request is null || string.IsNullOrWhiteSpace(request.Operation))
        {
            ServerLog.Fail(clientId.Name, "Invalid request: Operation is missing");
            return Serialize(Response.Error("Invalid request format: Operation is missing"));
        }

        // Имя из первого запроса клиента используется во всех остальных строках журнала.
        if (clientId.Announce(request.ClientName))
            ServerLog.Info(clientId.Name, $"→ CLIENT NAME (server id was {clientId.FallbackName})");
        string who = clientId.Name;

        try
        {
            string payload = request.Payload ?? string.Empty;

            Response response = request.Operation switch
            {
                "CheckAvailability" => await _logic.CheckAvailabilityAsync(payload, who),
                "CreateBooking" => await _logic.CreateBookingAsync(payload, who),
                "PayBooking" => _logic.PayBooking(payload, who),
                "CancelBooking" => _logic.CancelBooking(payload, who),
                "GetServerTime" => await TimeService.GetServerTimeAsync(payload, who),
                "DatabaseSnapshot" => DatabaseOperation(db => db.DatabaseSnapshot(payload, who), who),
                "ResetDatabase" => DatabaseOperation(db => db.ResetDatabase(payload, who), who),
                "CommitFailureCheck" => DatabaseOperation(db => db.CommitFailureCheck(payload, who), who),
                "DatabaseHealth" => DatabaseOperation(db => db.DatabaseHealth(payload, who), who),
                "InstanceStatus" => ReplicationOperation(r => r.InstanceStatus(payload, who), who),
                "FailInstance" => ReplicationOperation(r => r.FailInstance(payload, who), who),
                "RestoreInstance" => ReplicationOperation(r => r.RestoreInstance(payload, who), who),
                "CompareReplicas" => ReplicationOperation(r => r.CompareReplicas(payload, who), who),
                "SynchronizeReplicas" => ReplicationOperation(r => r.SynchronizeReplicas(payload, who), who),
                "PubSubStatus" => PubSubOperation(m => m.PubSubStatus(payload, who), who),
                "PublishEvent" => PubSubOperation(m => m.PublishEvent(payload, who), who),
                "AddSubscriber" => PubSubOperation(m => m.AddSubscriber(payload, who), who),
                "SubscribeEvent" => PubSubOperation(m => m.SubscribeEvent(payload, who), who),
                "UnsubscribeEvent" => PubSubOperation(m => m.UnsubscribeEvent(payload, who), who),
                "FailSubscriber" => PubSubOperation(m => m.FailSubscriber(payload, who), who),
                "RestoreSubscriber" => PubSubOperation(m => m.RestoreSubscriber(payload, who), who),
                "FailBroker" => PubSubOperation(m => m.FailBroker(payload, who), who),
                "RestoreBroker" => PubSubOperation(m => m.RestoreBroker(payload, who), who),
                "RetryPending" => PubSubOperation(m => m.RetryPending(payload, who), who),
                "EventStatistics" => PubSubOperation(m => m.EventStatistics(payload, who), who),
                "ClusterStatus" => ConsensusOperation(c => c.ClusterStatus(payload, who), who),
                "StartElection" => ConsensusOperation(c => c.StartElection(payload, who), who),
                "HeartbeatProbe" => ConsensusOperation(c => c.HeartbeatProbe(payload, who), who),
                "FailNode" => ConsensusOperation(c => c.FailNode(payload, who), who),
                "RestoreNode" => ConsensusOperation(c => c.RestoreNode(payload, who), who),
                "ProposeChange" => ConsensusOperation(c => c.ProposeChange(payload, who), who),
                "SendNodeMessage" => ConsensusOperation(c => c.SendNodeMessage(payload, who), who),
                "BroadcastNodes" => ConsensusOperation(c => c.BroadcastNodes(payload, who), who),
                "ClusterJournal" => ConsensusOperation(c => c.ClusterJournal(payload, who), who),
                "ClusterStatistics" => ConsensusOperation(c => c.ClusterStatistics(payload, who), who),
                _ => UnknownOperation(request.Operation, who)
            };

            return Serialize(response);
        }
        catch (Exception ex)
        {
            ServerLog.Error(who, $"{ex.GetType().Name}: {ex.Message}");
            return Serialize(Response.Error($"Server error: {ex.Message}"));
        }
    }

    /// <summary>Операции, доступные только серверу с базой данных (ЛР №4).</summary>
    private Response DatabaseOperation(Func<IDatabaseBookingApi, Response> operation, string clientId) =>
        _logic is IDatabaseBookingApi database
            ? operation(database)
            : Response.Error("This server keeps data in memory; start it with --storage postgres to use the database");

    /// <summary>Операции двух копий данных (ЛР №5): доступны серверу, запущенному с --replication on.</summary>
    private Response ReplicationOperation(Func<IReplicationBookingApi, Response> operation, string clientId) =>
        _logic is IReplicationBookingApi replication
            ? operation(replication)
            : Response.Error("This server has a single data instance; start it with --storage postgres --replication on");

    /// <summary>Операции событийного механизма (ЛР №6): доступны серверу, запущенному с --pubsub on.</summary>
    private Response PubSubOperation(Func<IPubSubBookingApi, Response> operation, string clientId) =>
        _logic is IPubSubBookingApi pubSub
            ? operation(pubSub)
            : Response.Error("This server has no event mechanism; start it with --pubsub on");

    /// <summary>Операции кластера (ЛР №7): доступны серверу, запущенному с --cluster on.</summary>
    private Response ConsensusOperation(Func<IConsensusBookingApi, Response> operation, string clientId) =>
        _logic is IConsensusBookingApi consensus
            ? operation(consensus)
            : Response.Error("This server has no node cluster; start it with --cluster on");

    private static Response UnknownOperation(string operation, string clientId)
    {
        ServerLog.Fail(clientId, $"Unknown operation '{operation}'");
        return Response.Error($"Unknown operation: '{operation}'");
    }

    private static string Serialize(Response response) => JsonSerializer.Serialize(response);
}
