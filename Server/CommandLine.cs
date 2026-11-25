namespace Server;

using Server.Database;

public static class CommandLine
{
    /// <summary>Задержка обработки события подписчиком: "Logger=1000" - имя и миллисекунды через «=».</summary>
    private static void ApplySubscriberDelay(string value)
    {
        string[] parts = value.Split('=', 2);

        if (parts.Length != 2 || !int.TryParse(parts[1], out int delay) || delay < 0)
        {
            Console.WriteLine($"[SERVER] Ignored --event-delay '{value}', expected format Name=milliseconds");
            return;
        }

        ServerConfig.SubscriberDelaysMs[parts[0].Trim()] = delay;
    }

    public static void Apply(string[] args)
    {
        // ЛР №8: сначала окружение контейнера, затем аргументы - они перекрывают переменные.
        EnvironmentConfig.Apply();

        for (int i = 0; i < args.Length - 1; i++)
        {
            switch (args[i])
            {
                case "--port":
                    if (int.TryParse(args[++i], out int port))
                        ServerConfig.Port = port;
                    break;
                case "--reserve-delay":
                    if (int.TryParse(args[++i], out int reserveDelay) && reserveDelay >= 0)
                        ServerConfig.ReserveProcessingDelayMs = reserveDelay;
                    break;
                case "--time-delay":
                    if (int.TryParse(args[++i], out int timeDelay) && timeDelay >= 0)
                        ServerConfig.DefaultTimeResponseDelayMs = timeDelay;
                    break;
                case "--sync":
                    ServerConfig.SyncMode = args[++i].ToLowerInvariant();
                    break;
                case "--storage":
                    ServerConfig.Storage = args[++i].ToLowerInvariant();
                    break;
                case "--db-conn":
                    DatabaseConfig.ConnectionStringOverride = args[++i];
                    break;
                case "--tx":
                    ServerConfig.UseTransactionByDefault =
                        !args[++i].Equals("off", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--race-window":
                    if (int.TryParse(args[++i], out int raceWindow) && raceWindow >= 0)
                        ServerConfig.RaceWindowMs = raceWindow;
                    break;
                case "--replication":
                    ServerConfig.ReplicationEnabled = args[++i].Equals("on", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--primary-schema":
                    DatabaseConfig.PrimarySchema = args[++i].ToLowerInvariant();
                    break;
                case "--replica-schema":
                    DatabaseConfig.ReplicaSchema = args[++i].ToLowerInvariant();
                    break;
                case "--primary-conn":
                    DatabaseConfig.PrimaryConnection = args[++i];
                    break;
                case "--replica-conn":
                    DatabaseConfig.ReplicaConnection = args[++i];
                    break;
                case "--pubsub":
                    ServerConfig.PubSubEnabled = args[++i].Equals("on", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--event-delay":
                    ApplySubscriberDelay(args[++i]);
                    break;
                case "--event-retries":
                    if (int.TryParse(args[++i], out int attempts) && attempts > 0)
                        ServerConfig.EventDeliveryAttempts = attempts;
                    break;
                case "--cluster":
                    ServerConfig.ConsensusEnabled = args[++i].Equals("on", StringComparison.OrdinalIgnoreCase);
                    break;
                case "--nodes":
                    if (int.TryParse(args[++i], out int nodes) && nodes >= 3)
                        ServerConfig.ClusterNodeCount = nodes;
                    break;
                case "--cluster-port":
                    if (int.TryParse(args[++i], out int basePort) && basePort > 0)
                        ServerConfig.ClusterBasePort = basePort;
                    break;
                case "--heartbeat-timeout":
                    if (int.TryParse(args[++i], out int heartbeat) && heartbeat >= 50)
                        ServerConfig.HeartbeatTimeoutMs = heartbeat;
                    break;
                default:
                    Console.WriteLine($"[SERVER] Ignored unknown argument: {args[i]}");
                    break;
            }
        }

        // Единственная поддерживаемая СУБД — PostgreSQL; "memory" оставлен для лабораторных №2 и №3.
        if (ServerConfig.Storage is "postgresql")
            ServerConfig.Storage = "postgres";
        else if (ServerConfig.Storage is not ("memory" or "postgres"))
        {
            Console.WriteLine($"[SERVER] Unknown storage '{ServerConfig.Storage}', supported: memory, postgres — using memory");
            ServerConfig.Storage = "memory";
        }

        Console.WriteLine($"[SERVER] pubsub={(ServerConfig.PubSubEnabled ? "on" : "off")} " +
                          $"subscribers = Logger {ServerConfig.SubscriberDelaysMs["Logger"]} ms, " +
                          $"RoomService {ServerConfig.SubscriberDelaysMs["RoomService"]} ms, " +
                          $"NotificationService {ServerConfig.SubscriberDelaysMs["NotificationService"]} ms, " +
                          $"delivery attempts = {ServerConfig.EventDeliveryAttempts}");

        Console.WriteLine($"[SERVER] cluster={(ServerConfig.ConsensusEnabled ? "on" : "off")} " +
                          $"nodes = {ServerConfig.ClusterNodeCount} on ports " +
                          $"{ServerConfig.ClusterBasePort}-{ServerConfig.ClusterBasePort + ServerConfig.ClusterNodeCount - 1}, " +
                          $"majority = {ServerConfig.ClusterNodeCount / 2 + 1}, " +
                          $"heartbeat timeout = {ServerConfig.HeartbeatTimeoutMs} ms");

        string target = ServerConfig.UseDatabase ? $"{DatabaseConfig.EngineName} → {DatabaseConfig.Target}" : "memory";
        string replication = ServerConfig.UseDatabase
            ? $"replication={(ServerConfig.ReplicationEnabled ? "on" : "off")} primary={DatabaseConfig.PrimarySchema} replica={DatabaseConfig.ReplicaSchema}"
            : "replication=off";

        Console.WriteLine($"[SERVER] appMode={ServerConfig.AppMode}");
        Console.WriteLine($"[SERVER] port={ServerConfig.Port} reserveDelayMs={ServerConfig.ReserveProcessingDelayMs} " +
                          $"timeDelayMs={ServerConfig.DefaultTimeResponseDelayMs} sync={ServerConfig.SyncMode} " +
                          $"storage={target} transaction={ServerConfig.UseTransactionByDefault} {replication}");
    }
}
