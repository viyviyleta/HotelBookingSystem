using System.Text.Json.Serialization;

namespace Server.Replication;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum InstanceRole
{
    Primary,
    Replica
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum InstanceState
{
    Available,
    Unavailable
}

public sealed record InstanceStatus(
    InstanceRole Role,
    string Schema,
    string Target,
    InstanceState State,
    string Reason,
    string HealthProbe);

public sealed record ReplicationOutcome(
    bool IsSuccess,
    string Message,
    InstanceRole Source,
    InstanceRole Target,
    int BookingRows,
    int RoomRows,
    int PaymentRows,
    string? Error);

public sealed record InstanceSnapshot(
    InstanceRole Role,
    string Schema,
    string Target,
    int Rooms,
    int AvailableRooms,
    int Bookings,
    int ActiveBookings,
    int Payments,
    bool IsAvailable,
    string Fingerprint);

public sealed record ConsistencyReport(
    bool IsConsistent,
    string Summary,
    IReadOnlyList<string> Differences,
    InstanceSnapshot Primary,
    InstanceSnapshot Replica);

public sealed record ClusterReport(
    bool ReplicationEnabled,
    string Serving,
    string Mode,
    IReadOnlyList<InstanceStatus> Instances,
    ConsistencyReport? Consistency,
    string Summary);

public sealed class InstanceCommand
{
    public string Role { get; set; } = "primary";
}

public sealed class SynchronizeCommand
{
    public string From { get; set; } = "primary";
    public string To { get; set; } = "replica";
}

public static class Roles
{
    public static bool TryParse(string? value, out InstanceRole role)
    {
        role = value?.Trim().ToLowerInvariant() switch
        {
            "primary" => InstanceRole.Primary,
            "replica" => InstanceRole.Replica,
            _ => (InstanceRole)(-1)
        };

        return role != (InstanceRole)(-1);
    }

    public static string Name(InstanceRole role) => role == InstanceRole.Primary ? "primary" : "replica";
}
