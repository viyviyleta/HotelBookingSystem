using System.Data.Common;
using System.Text.Json;
using Npgsql;
using Server;
using Server.Database;
using Server.Replication;
using Shared.Network;

namespace Tests;

/// <summary>
/// Общие инструменты тестов ЛР №5. Каждая проверка поднимает свой кластер «Primary + Replica»
/// из двух схем в одной базе PostgreSQL, поэтому копии тестов не пересекаются ни между собой,
/// ни со схемой public, где вручную прогоняются эксперименты.
/// </summary>
internal static class TestCluster
{
    private const string MissingDatabase = "riot-lr5-missing";

    private static string? _primarySchema;
    private static string? _replicaSchema;

    /// <summary>Кластер с двумя живыми копиями: обе созданы и заполнены, Replica получает данные с Primary.</summary>
    public static DbBookingService Create(string label, bool replication = true)
    {
        Configure(label);

        var cluster = new InstanceCluster(
            new DatabaseInstance(InstanceRole.Primary, _primarySchema!),
            new DatabaseInstance(InstanceRole.Replica, _replicaSchema!),
            replication);

        cluster.Initialize("tester");

        return new DbBookingService(cluster);
    }

    /// <summary>
    /// Кластер, у которого Replica подключается к несуществующей базе: копия отвечает настоящей
    /// ошибкой соединения (3D000), а не только пометкой приложения.
    /// </summary>
    public static DbBookingService CreateWithUnreachableReplica(string label)
    {
        Configure(label);

        var primary = new DatabaseInstance(InstanceRole.Primary, _primarySchema!);
        var replica = new DatabaseInstance(InstanceRole.Replica, _replicaSchema!)
        {
            ConnectionOverride = $"Host={DatabaseConfig.Host};Port={DatabaseConfig.Port};Database={MissingDatabase};Username={DatabaseConfig.User}"
        };

        primary.Initialize();

        return new DbBookingService(new InstanceCluster(primary, replica, replicationEnabled: true));
    }

    public static string PrimarySchema => _primarySchema!;

    public static string ReplicaSchema => _replicaSchema!;

    /// <summary>Снимок копии через её собственный экземпляр: отказанную копию прочитать нельзя.</summary>
    public static InstanceSnapshot ReadOf(DbBookingService service, InstanceRole role)
    {
        InstanceCluster cluster = service.Cluster!;
        return cluster.Replicator.ReadCopy(cluster[role], "tester");
    }

    /// <summary>Снимок «со стороны»: содержимое схемы читается даже тогда, когда копия помечена недоступной.</summary>
    public static InstanceSnapshot PeekOf(DbBookingService service, InstanceRole role)
    {
        InstanceCluster cluster = service.Cluster!;
        return cluster.Replicator.PeekCopy(cluster[role]);
    }

    /// <summary>Сверка содержимого двух копий по «отпечатку» строк.</summary>
    public static ConsistencyReport Compare(DbBookingService service)
    {
        InstanceCluster cluster = service.Cluster!;
        return cluster.Replicator.Compare(cluster.Primary, cluster.Replica, "tester");
    }

    public static string RolePayload(string role) => JsonSerializer.Serialize(new { Role = role });

    public static string SyncPayload(string from, string to) => JsonSerializer.Serialize(new { From = from, To = to });

    public static T Data<T>(Response response) where T : class =>
        JsonSerializer.Deserialize<T>(response.Data,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;

    public static void Cleanup()
    {
        string[] schemas = (new[] { _primarySchema, _replicaSchema }).OfType<string>().ToArray();

        _primarySchema = null;
        _replicaSchema = null;

        DatabaseConfig.ConnectionStringOverride = null;
        DatabaseConfig.Schema = null;
        DatabaseConfig.PrimarySchema = "riot_primary";
        DatabaseConfig.ReplicaSchema = "riot_replica";
        DatabaseConfig.PrimaryConnection = null;
        DatabaseConfig.ReplicaConnection = null;
        ServerConfig.ReplicationEnabled = false;

        foreach (string schema in schemas)
        {
            try
            {
                Execute($"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
            }
            catch (PostgresException)
            {
                // схема осталась в базе — на результат теста не влияет, имя у каждого теста своё
            }
        }

        // У каждого теста своя схема, то есть своя строка подключения, а пул держит соединения
        // даже после Dispose — без очистки сессий следующий тест упирается в 53300 «too many clients».
        NpgsqlConnection.ClearAllPools();
    }

    private static void Configure(string label)
    {
        Cleanup();

        // Имя схемы ограничено 63 символами, поэтому метка теста укорочена.
        string stem = $"riot_lr5_{Slug(label, 12)}_{Guid.NewGuid():N}";
        _primarySchema = $"{stem}_p";
        _replicaSchema = $"{stem}_r";

        DatabaseConfig.PrimarySchema = _primarySchema;
        DatabaseConfig.ReplicaSchema = _replicaSchema;

        ServerConfig.Storage = "postgres";
        ServerConfig.ReplicationEnabled = true;
        ServerConfig.SyncMode = "lock";
        ServerConfig.UseTransactionByDefault = true;
        ServerConfig.ReserveProcessingDelayMs = 10;
        ServerConfig.RaceWindowMs = 60;
    }

    /// <summary>Запись в схему напрямую, мимо сервиса: нужна, чтобы задать расхождение копий.</summary>
    public static void Execute(string sql)
    {
        using var connection = new NpgsqlConnection(DatabaseConfig.BuildConnectionString());
        connection.Open();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static string Slug(string label, int maxLength)
    {
        string slug = new(label.ToLowerInvariant().Where(char.IsLetterOrDigit).ToArray());
        return slug.Length <= maxLength ? slug : slug[..maxLength];
    }
}
