using Server.Consensus;
using Server.Database;
using Server.Messaging;
using Server.Replication;

namespace Server;

internal class Program
{
    private static async Task Main(string[] args)
    {
        CommandLine.Apply(args);

        ClusterManager? nodes = CreateCluster();
        IBookingApi logic = ServerConfig.UseDatabase ? CreateDatabaseService(nodes) : CreateMemoryLogic();

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var listener = new ServerListener(ServerConfig.Port, new RequestHandler(logic));

        try
        {
            await listener.StartAsync(cts.Token);
        }
        finally
        {
            nodes?.Stop();
        }
    }

    private static IBookingApi CreateMemoryLogic() => ServerConfig.SyncMode switch
    {
        "none" => new NoSynchronizationBusinessLogic(),
        _ => new LockBusinessLogic()
    };

    /// <summary>
    /// Сервер создаёт базу данных при старте. Если соединение недоступно, сервер
    /// не завершается: он продолжает принимать клиентов, а каждая операция с БД
    /// возвращает клиенту текст ошибки подключения.
    /// </summary>
    private static IBookingApi CreateDatabaseService(ClusterManager? nodes)
    {
        MessagingHub? hub = CreateMessagingHub();

        if (!ServerConfig.ReplicationEnabled)
            return new DbBookingService(new BookingStore(CreateSingleDatabase()), hub, nodes);

        var cluster = new InstanceCluster(
            new DatabaseInstance(InstanceRole.Primary, DatabaseConfig.PrimarySchema!)
            {
                ConnectionOverride = DatabaseConfig.PrimaryConnection
            },
            new DatabaseInstance(InstanceRole.Replica, DatabaseConfig.ReplicaSchema!)
            {
                ConnectionOverride = DatabaseConfig.ReplicaConnection
            },
            replicationEnabled: true);

        try
        {
            cluster.Initialize("SERVER");
            Console.WriteLine($"[SERVER] {DatabaseConfig.EngineName} cluster ready: {cluster.Mode}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVER] WARNING: cluster is not fully ready — {ex.Message}");
            Console.WriteLine("[SERVER] WARNING: server keeps running, every database operation will report this error");
        }

        return new DbBookingService(cluster, hub, nodes);
    }

    /// <summary>
    /// Кластер узлов (ЛР №7) поднимается вместе с сервером: узлы на своих портах и первая выборка лидера.
    /// Сбой этого слоя не мешает серверу принимать клиентов - просто записи идут без согласования.
    /// </summary>
    private static ClusterManager? CreateCluster()
    {
        if (!ServerConfig.ConsensusEnabled)
            return null;

        if (!ServerConfig.UseDatabase)
        {
            Console.WriteLine("[SERVER] WARNING: --cluster on works for the database server " +
                              "(--storage postgres), the node cluster is disabled");
            return null;
        }

        ClusterManager? cluster = null;

        try
        {
            cluster = ClusterManager.CreateDefault("SERVER", ServerConfig.ClusterNodeCount,
                ServerConfig.ClusterBasePort, ServerConfig.HeartbeatTimeoutMs);

            Console.WriteLine("[SERVER] " + cluster.Status().Summary);
            return cluster;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVER] WARNING: the node cluster is not available — {ex.Message}");
            Console.WriteLine("[SERVER] WARNING: server keeps running, writes simply skip the consensus");
            cluster?.Stop();
            return null;
        }
    }

    /// <summary>
    /// Механизм событий поднимается вместе с сервером: Broker-и, Publisher и подписчики из
    /// конфигурации. Сбой этого слоя не должен мешать серверу принимать клиентов.
    /// </summary>
    private static MessagingHub? CreateMessagingHub()
    {
        if (!ServerConfig.PubSubEnabled)
            return null;

        if (!ServerConfig.UseDatabase)
        {
            Console.WriteLine("[SERVER] WARNING: --pubsub on works for the database server " +
                              "(--storage postgres), events are disabled for in-memory storage");
            return null;
        }

        try
        {
            MessagingHub hub = MessagingHub.CreateDefault("SERVER");
            Console.WriteLine("[SERVER] Pub/Sub ready: Publisher → " +
                              string.Join(", ", hub.BrokerNames) + "; subscribers = " +
                              string.Join(", ", hub.Subscribers.Select(subscriber => subscriber.Name)));
            return hub;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVER] WARNING: the event mechanism is not available — {ex.Message}");
            Console.WriteLine("[SERVER] WARNING: server keeps running, domain operations simply publish nothing");
            return null;
        }
    }

    private static HotelDatabase CreateSingleDatabase()
    {
        var database = new HotelDatabase();

        try
        {
            database.Initialize();
            Console.WriteLine($"[SERVER] {DatabaseConfig.EngineName} database ready: {database.DatabaseTarget}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVER] WARNING: database is unavailable — {ex.Message}");
            Console.WriteLine("[SERVER] WARNING: server keeps running, every database operation will report this error");
        }

        return database;
    }
}
