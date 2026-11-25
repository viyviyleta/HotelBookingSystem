using Server.Database;
using Shared.Configuration;

namespace Server;

/// <summary>
/// ЛР №8: параметры запуска из переменных окружения контейнера. Один и тот же образ
/// работает и на хосте, и в Docker Compose - меняется только окружение, не исходный код.
/// Значение, которое не удалось разобрать, отбрасывается с предупреждением: сервер
/// продолжает запускаться на прежнем значении, а не падает на старте.
/// </summary>
public static class EnvironmentConfig
{
    public static void Apply()
    {
        ServerConfig.AppMode = ReadAppMode();
        ServerConfig.Port = Int("SERVER_PORT", ServerConfig.Port, 1, 65535);
        ServerConfig.ReserveProcessingDelayMs = Int("RESERVE_DELAY_MS", ServerConfig.ReserveProcessingDelayMs, 0, 600000);
        ServerConfig.SyncMode = Env.Text(Environment.GetEnvironmentVariable("SERVER_SYNC"), ServerConfig.SyncMode);
        ServerConfig.Storage = Env.Text(Environment.GetEnvironmentVariable("SERVER_STORAGE"), ServerConfig.Storage);
        ServerConfig.ReplicationEnabled = Bool("REPLICATION", ServerConfig.ReplicationEnabled);
        ServerConfig.PubSubEnabled = Bool("PUBSUB", ServerConfig.PubSubEnabled);
        ServerConfig.ConsensusEnabled = Bool("CLUSTER", ServerConfig.ConsensusEnabled);
        ServerConfig.ClusterNodeCount = Int("CLUSTER_NODES", ServerConfig.ClusterNodeCount, 3, 15);
        ServerConfig.ClusterBasePort = Int("CLUSTER_PORT", ServerConfig.ClusterBasePort, 1, 65535);
        ServerConfig.HeartbeatTimeoutMs = Int("HEARTBEAT_TIMEOUT_MS", ServerConfig.HeartbeatTimeoutMs, 50, 60000);
        ServerConfig.EventDeliveryAttempts = Int("EVENT_RETRIES", ServerConfig.EventDeliveryAttempts, 1, 10);

        DatabaseConfig.Host = Env.Text(Environment.GetEnvironmentVariable("DB_HOST"), DatabaseConfig.Host);
        DatabaseConfig.Port = Int("DB_PORT", DatabaseConfig.Port, 1, 65535);
        DatabaseConfig.Database = Env.Text(Environment.GetEnvironmentVariable("DB_NAME"), DatabaseConfig.Database);
        DatabaseConfig.User = Env.Text(Environment.GetEnvironmentVariable("DB_USER"), DatabaseConfig.User);
        DatabaseConfig.Password = Env.Text(Environment.GetEnvironmentVariable("DB_PASSWORD"), DatabaseConfig.Password);
        DatabaseConfig.PrimarySchema = Env.Text(Environment.GetEnvironmentVariable("DB_PRIMARY_SCHEMA"), DatabaseConfig.PrimarySchema!);
        DatabaseConfig.ReplicaSchema = Env.Text(Environment.GetEnvironmentVariable("DB_REPLICA_SCHEMA"), DatabaseConfig.ReplicaSchema!);
    }

    /// <summary>ЛР №9: значение APP_MODE из ConfigMap; пустая переменная означает режим по умолчанию.</summary>
    public static string ResolveAppMode(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "default" : value.Trim();

    /// <summary>ЛР №9: чтение параметра ConfigMap из окружения контейнера.</summary>
    public static string ReadAppMode() =>
        ResolveAppMode(Environment.GetEnvironmentVariable("APP_MODE"));

    private static int Int(string name, int fallback, int min, int max)
    {
        string? value = Environment.GetEnvironmentVariable(name);

        if (Env.TryNumber(value, min, max, out int number))
            return number;

        if (!string.IsNullOrWhiteSpace(value))
            Console.WriteLine($"[SERVER] Ignored {name}='{value}': expected a whole number in {min}..{max}, using {fallback}");

        return fallback;
    }

    private static bool Bool(string name, bool fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);

        if (Env.TryFlag(value, out bool enabled))
            return enabled;

        if (!string.IsNullOrWhiteSpace(value))
            Console.WriteLine($"[SERVER] Ignored {name}='{value}': expected on or off, using {(fallback ? "on" : "off")}");

        return fallback;
    }
}