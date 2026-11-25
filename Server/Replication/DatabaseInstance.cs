using System.Data.Common;
using Server.Database;

namespace Server.Replication;

public sealed class DatabaseInstance : HotelDatabase
{
    private readonly InstanceRole _role;

    public DatabaseInstance(InstanceRole role, string schema) : base(Validate(schema))
    {
        _role = role;
        Schema = Validate(schema);
    }

    public string Schema { get; }

    public string? ConnectionOverride { get; init; }

    public InstanceRole Role => _role;

    public InstanceState State { get; private set; } = InstanceState.Available;

    public string Reason { get; private set; } = "started, no failures recorded";

    public DateTime? FailedAtUtc { get; private set; }

    public bool IsAvailable => State == InstanceState.Available;

    public string Label => _role.ToString();

    public string Qualify(string table) => $"{Schema}.{table}";

    protected override string ConnectionString => DatabaseConfig.BuildConnectionString(Schema, ConnectionOverride);

    public override string DatabaseTarget => DatabaseConfig.TargetFor(Schema, ConnectionOverride);

    public override DbConnection Open()
    {
        if (!IsAvailable)
            throw new InstanceUnavailableException(
                $"{Label} instance is UNAVAILABLE (schema '{Schema}'): {Reason}");

        return base.Open();
    }

    public void Fail(string reason)
    {
        State = InstanceState.Unavailable;
        Reason = reason;
        FailedAtUtc = DateTime.UtcNow;
    }

    public void Restore(string reason)
    {
        State = InstanceState.Available;
        Reason = reason;
        FailedAtUtc = null;
    }

    /// Проверка реального соединения, независимо от состояния, которое установило приложение.
    public (bool Connected, string Detail) Probe()
    {
        try
        {
            using DbConnection connection = base.Open();
            object? rooms = Scalar(connection, null, "SELECT COUNT(*) FROM rooms");
            return (true, $"connected, rooms = {rooms}");
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    public InstanceStatus Inspect()
    {
        (bool connected, string probe) = Probe();

        if (IsAvailable && !connected)
        {
            Fail($"connection error detected by health check: {probe}");
        }

        return new InstanceStatus(
            _role,
            Schema,
            DatabaseTarget,
            State,
            Reason,
            connected ? probe : $"NOT CONNECTED — {probe}");
    }

    private static string Validate(string schema)
    {
        if (string.IsNullOrWhiteSpace(schema) || !IsSafeIdentifier(schema))
            throw new ArgumentException(
                $"schema name '{schema}' is not a safe PostgreSQL identifier (expected [a-z][a-z0-9_]*)", nameof(schema));

        return schema;
    }
}
