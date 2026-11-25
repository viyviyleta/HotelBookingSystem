using Server.Database;

namespace Server.Replication;

public sealed class InstanceCluster
{
    public DatabaseInstance Primary { get; }

    public DatabaseInstance Replica { get; }

    public Replicator Replicator { get; } = new();

    public bool ReplicationEnabled { get; }

    public InstanceCluster(DatabaseInstance primary, DatabaseInstance replica, bool replicationEnabled)
    {
        Primary = primary;
        Replica = replica;
        ReplicationEnabled = replicationEnabled;
    }

    public string Mode => ReplicationEnabled ? "Primary + Replica (replication ON)" : "Primary only (replication OFF)";

    public void Initialize(string clientId)
    {
        Primary.Initialize();
        ServerLog.Info(clientId, $"→ [INSTANCE] Primary ready: {Primary.DatabaseTarget}");

        Replica.Initialize();
        ServerLog.Info(clientId, $"→ [INSTANCE] Replica ready: {Replica.DatabaseTarget}");

        if (ReplicationEnabled)
            Replicator.Synchronize(Primary, Replica, clientId);
    }

    public DatabaseInstance this[InstanceRole role] => role == InstanceRole.Primary ? Primary : Replica;

    public DatabaseInstance Other(DatabaseInstance instance) =>
        ReferenceEquals(instance, Primary) ? Replica : Primary;

    public IReadOnlyList<InstanceStatus> Status() => new[] { Primary.Inspect(), Replica.Inspect() };

    /// Штатный маршрут — Primary; при его отказе — Replica; при отказе обеих — ошибка без падения сервера.
    public DatabaseInstance Route(string clientId)
    {
        if (IsServing(Primary, clientId))
            return Announce(clientId, Primary, "normal operation");

        ReplicationLog.Failover(clientId,
            $"START — {Primary.Label} is UNAVAILABLE ({Primary.Reason}), switching to {Replica.Label}");

        if (IsServing(Replica, clientId))
        {
            ReplicationLog.Failover(clientId, $"SUCCESS — request redirected to {Replica.Label}");
            return Announce(clientId, Replica, "after failover");
        }

        ReplicationLog.Failover(clientId, "IMPOSSIBLE — both instances are unavailable");

        throw new InstanceUnavailableException(
            $"both data instances are UNAVAILABLE — Primary: {Primary.Reason}; Replica: {Replica.Reason}");
    }

    /// Кому пересылать изменение: второй экземпляр, если репликация включена.
    public DatabaseInstance? CounterpartOf(DatabaseInstance served) =>
        ReplicationEnabled ? Other(served) : null;

    private static bool IsServing(DatabaseInstance instance, string clientId)
    {
        ReplicationLog.Check(clientId, instance.Label);

        InstanceStatus status = instance.Inspect();

        ReplicationLog.InstanceState(clientId, status.Role.ToString(),
            status.State == InstanceState.Available ? "AVAILABLE" : "UNAVAILABLE",
            $"schema {status.Schema}, {status.HealthProbe}");

        return status.State == InstanceState.Available;
    }

    private static DatabaseInstance Announce(string clientId, DatabaseInstance instance, string why)
    {
        ReplicationLog.Redirected(clientId, $"{instance.Label} (schema {instance.Schema}) — {why}");
        return instance;
    }
}
