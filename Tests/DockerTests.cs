using Server;
using Server.Database;
using Xunit;

namespace Tests;

/// <summary>
/// ЛР №8: проверки Docker-конфигурации и параметров окружения. Тесты не запускают Docker,
/// а проверяют то, что от него не зависит: Dockerfile, compose.yaml, .dockerignore и разбор
/// переменных окружения. Логика предметной области проверяется остальными наборами тестов.
/// </summary>
public sealed class DockerTests
{
    private static readonly string Root = FindRoot();

    private static string FindRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "HotelBookingSystem.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Не найден корень проекта HotelBookingSystem.slnx");
    }

    private static string Read(string relativePath) =>
        File.ReadAllText(Path.Combine(Root, relativePath));

    // ------------------------------------------------------------ Dockerfile

    [Fact]
    public void ServerDockerfile_ContainsRequiredInstructionsAndMultiStageBuild()
    {
        string dockerfile = Read("Server/Dockerfile");

        Assert.Contains("FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build", dockerfile);
        Assert.Contains("FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final", dockerfile);
        Assert.Contains("WORKDIR", dockerfile);
        Assert.Contains("COPY Shared/Shared.csproj", dockerfile);
        Assert.Contains("COPY Server/Server.csproj", dockerfile);
        Assert.Contains("dotnet restore Server/Server.csproj", dockerfile);
        Assert.Contains("COPY Shared/ Shared/", dockerfile);
        Assert.Contains("COPY Server/ Server/", dockerfile);
        Assert.Contains("dotnet publish Server/Server.csproj -c Release -o /app/publish", dockerfile);
        Assert.Contains("EXPOSE 5377", dockerfile);
        Assert.Contains("ENTRYPOINT [\"dotnet\", \"Server.dll\"]", dockerfile);
    }

    [Fact]
    public void ClientDockerfile_ContainsRequiredInstructionsAndMultiStageBuild()
    {
        string dockerfile = Read("Client/Dockerfile");

        Assert.Contains("FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build", dockerfile);
        Assert.Contains("FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final", dockerfile);
        Assert.Contains("WORKDIR", dockerfile);
        Assert.Contains("dotnet restore Client/Client.csproj", dockerfile);
        Assert.Contains("dotnet publish Client/Client.csproj -c Release -o /app/publish", dockerfile);
        Assert.Contains("ENTRYPOINT [\"dotnet\", \"Client.dll\"]", dockerfile);
    }

    [Fact]
    public void Dockerfiles_DoNotBakeSecretsIntoTheImage()
    {
        foreach (string file in new[] { "Server/Dockerfile", "Client/Dockerfile" })
            Assert.DoesNotContain("DB_PASSWORD=", Read(file), StringComparison.OrdinalIgnoreCase);
    }

    // ------------------------------------------------------------ compose.yaml

    [Fact]
    public void ComposeFile_DescribesDatabaseServerAndClient()
    {
        string compose = Read("compose.yaml");

        Assert.Contains("services:", compose);
        Assert.Contains("db:", compose);
        Assert.Contains("server:", compose);
        Assert.Contains("client:", compose);

        Assert.Contains("image: postgres:17-alpine", compose);
        Assert.Contains("dockerfile: Server/Dockerfile", compose);
        Assert.Contains("dockerfile: Client/Dockerfile", compose);
        Assert.Contains("riot-server:lr8", compose);
        Assert.Contains("riot-client:lr8", compose);
    }

    [Fact]
    public void ComposeFile_UsesNetworkVolumePortsAndEnvironment()
    {
        string compose = Read("compose.yaml");

        Assert.Contains("networks:", compose);
        Assert.Contains("riot-net:", compose);
        Assert.Contains("volumes:", compose);
        Assert.Contains("riot-db-data:", compose);

        Assert.Contains("POSTGRES_DB: ${DB_NAME:-hotelbooking}", compose);
        Assert.Contains("POSTGRES_USER: ${DB_USER:-postgres}", compose);
        Assert.Contains("POSTGRES_PASSWORD: ${DB_PASSWORD", compose);
        Assert.Contains("DB_HOST: db", compose);
        Assert.Contains("DB_PORT: 5432", compose);
        Assert.Contains("SERVER_HOST: server", compose);
        Assert.Contains("SERVER_PORT", compose);
        Assert.Contains("${SERVER_HOST_PORT:-5377}:${SERVER_PORT:-5377}", compose);

        Assert.Contains("pg_isready", compose);
        Assert.Contains("condition: service_healthy", compose);
        Assert.Contains("restart: unless-stopped", compose);
    }

    [Fact]
    public void ComposeFile_DoesNotContainTheRealDatabasePassword()
    {
        string compose = Read("compose.yaml");
        string env = Path.Combine(Root, ".env");

        if (!File.Exists(env))
            return;

        string? passwordLine = File.ReadLines(env)
            .FirstOrDefault(line => line.StartsWith("DB_PASSWORD=", StringComparison.Ordinal));

        if (passwordLine is null)
            return;

        string password = passwordLine["DB_PASSWORD=".Length..].Trim();

        if (password.Length > 0)
            Assert.DoesNotContain(password, compose);
    }

    // ------------------------------------------------------------ ignore-файлы

    [Fact]
    public void DockerIgnore_ExcludesBuildArtifactsSecretsAndTests()
    {
        string ignore = Read(".dockerignore");

        Assert.Contains("**/bin/", ignore);
        Assert.Contains("**/obj/", ignore);
        Assert.Contains("Tests/", ignore);
        Assert.Contains(".env", ignore);
    }

    [Fact]
    public void GitIgnore_ExcludesLocalEnvFile()
    {
        Assert.Contains(".env", Read(".gitignore"));
    }

    [Fact]
    public void EnvExample_ContainsPlaceholderPasswordOnly()
    {
        string example = Read(".env.example");

        Assert.Contains("DB_PASSWORD=change-me", example);
        Assert.Contains("DB_HOST_PORT", example);
        Assert.Contains("SERVER_PORT", example);
        Assert.Contains("CLUSTER_NODES", example);
    }

    // ------------------------------------------------------ переменные окружения

    [Fact]
    public void EnvironmentConfig_AppliesValuesFromContainerEnvironment()
    {
        ConfigSnapshot saved = ConfigSnapshot.Capture();

        try
        {
            Set("SERVER_PORT", "6001");
            Set("RESERVE_DELAY_MS", "250");
            Set("SERVER_SYNC", "none");
            Set("SERVER_STORAGE", "postgres");
            Set("REPLICATION", "on");
            Set("PUBSUB", "on");
            Set("CLUSTER", "on");
            Set("CLUSTER_NODES", "7");
            Set("CLUSTER_PORT", "5501");
            Set("HEARTBEAT_TIMEOUT_MS", "750");
            Set("EVENT_RETRIES", "5");
            Set("DB_HOST", "db");
            Set("DB_PORT", "5432");
            Set("DB_NAME", "hotelbooking");
            Set("DB_USER", "postgres");
            Set("DB_PASSWORD", "container-secret");
            Set("DB_PRIMARY_SCHEMA", "riot_primary");
            Set("DB_REPLICA_SCHEMA", "riot_replica");

            EnvironmentConfig.Apply();

            Assert.Equal(6001, ServerConfig.Port);
            Assert.Equal(250, ServerConfig.ReserveProcessingDelayMs);
            Assert.Equal("none", ServerConfig.SyncMode);
            Assert.Equal("postgres", ServerConfig.Storage);
            Assert.True(ServerConfig.ReplicationEnabled);
            Assert.True(ServerConfig.PubSubEnabled);
            Assert.True(ServerConfig.ConsensusEnabled);
            Assert.Equal(7, ServerConfig.ClusterNodeCount);
            Assert.Equal(5501, ServerConfig.ClusterBasePort);
            Assert.Equal(750, ServerConfig.HeartbeatTimeoutMs);
            Assert.Equal(5, ServerConfig.EventDeliveryAttempts);
            Assert.Equal("db", DatabaseConfig.Host);
            Assert.Equal(5432, DatabaseConfig.Port);
            Assert.Equal("hotelbooking", DatabaseConfig.Database);
            Assert.Equal("postgres", DatabaseConfig.User);
            Assert.Equal("container-secret", DatabaseConfig.Password);
            Assert.Equal("riot_primary", DatabaseConfig.PrimarySchema);
            Assert.Equal("riot_replica", DatabaseConfig.ReplicaSchema);
        }
        finally
        {
            ClearEnvironment();
            saved.Restore();
        }
    }

    [Fact]
    public void EnvironmentConfig_KeepsCurrentValuesWhenNumbersOrFlagsAreInvalid()
    {
        ConfigSnapshot saved = ConfigSnapshot.Capture();

        try
        {
            Set("SERVER_PORT", "not-a-port");
            Set("CLUSTER_NODES", "1");
            Set("HEARTBEAT_TIMEOUT_MS", "-5");
            Set("REPLICATION", "maybe");
            Set("CLUSTER", "sometimes");

            EnvironmentConfig.Apply();

            Assert.Equal(saved.Port, ServerConfig.Port);
            Assert.Equal(saved.ClusterNodeCount, ServerConfig.ClusterNodeCount);
            Assert.Equal(saved.HeartbeatTimeoutMs, ServerConfig.HeartbeatTimeoutMs);
            Assert.Equal(saved.ReplicationEnabled, ServerConfig.ReplicationEnabled);
            Assert.Equal(saved.ConsensusEnabled, ServerConfig.ConsensusEnabled);
        }
        finally
        {
            ClearEnvironment();
            saved.Restore();
        }
    }

    [Fact]
    public void DatabaseConfig_UsesDockerServiceNameInsteadOfLocalhost()
    {
        ConfigSnapshot saved = ConfigSnapshot.Capture();

        try
        {
            Set("DB_HOST", "db");
            Set("DB_PORT", "5432");
            Set("DB_NAME", "hotelbooking");
            Set("DB_USER", "postgres");
            Set("DB_PASSWORD", "container-secret");
            Set("DB_PRIMARY_SCHEMA", "riot_primary");

            EnvironmentConfig.Apply();

            string connection = DatabaseConfig.BuildConnectionString();

            Assert.Equal("db", DatabaseConfig.Host);
            Assert.Contains("Host=db", connection);
            Assert.DoesNotContain("localhost", connection, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Password=container-secret", connection);
            Assert.StartsWith("db:5432/hotelbooking", DatabaseConfig.Target);
        }
        finally
        {
            ClearEnvironment();
            saved.Restore();
        }
    }

    private static void Set(string name, string value) =>
        Environment.SetEnvironmentVariable(name, value);

    private static void ClearEnvironment()
    {
        foreach (string name in ConfigSnapshot.EnvironmentNames)
            Environment.SetEnvironmentVariable(name, null);
    }

    /// <summary>Снимок статических настроек: тест не должен менять состояние других тестов.</summary>
    private sealed record ConfigSnapshot(
        int Port,
        int ReserveProcessingDelayMs,
        string SyncMode,
        string Storage,
        bool ReplicationEnabled,
        bool PubSubEnabled,
        bool ConsensusEnabled,
        int ClusterNodeCount,
        int ClusterBasePort,
        int HeartbeatTimeoutMs,
        int EventDeliveryAttempts,
        string Host,
        int DatabasePort,
        string Database,
        string User,
        string Password,
        string? PrimarySchema,
        string? ReplicaSchema)
    {
        public static readonly string[] EnvironmentNames =
        [
            "SERVER_PORT", "RESERVE_DELAY_MS", "SERVER_SYNC", "SERVER_STORAGE", "REPLICATION",
            "PUBSUB", "CLUSTER", "CLUSTER_NODES", "CLUSTER_PORT", "HEARTBEAT_TIMEOUT_MS",
            "EVENT_RETRIES", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
            "DB_PRIMARY_SCHEMA", "DB_REPLICA_SCHEMA"
        ];

        public static ConfigSnapshot Capture() => new(
            ServerConfig.Port,
            ServerConfig.ReserveProcessingDelayMs,
            ServerConfig.SyncMode,
            ServerConfig.Storage,
            ServerConfig.ReplicationEnabled,
            ServerConfig.PubSubEnabled,
            ServerConfig.ConsensusEnabled,
            ServerConfig.ClusterNodeCount,
            ServerConfig.ClusterBasePort,
            ServerConfig.HeartbeatTimeoutMs,
            ServerConfig.EventDeliveryAttempts,
            DatabaseConfig.Host,
            DatabaseConfig.Port,
            DatabaseConfig.Database,
            DatabaseConfig.User,
            DatabaseConfig.Password,
            DatabaseConfig.PrimarySchema,
            DatabaseConfig.ReplicaSchema);

        public void Restore()
        {
            ServerConfig.Port = Port;
            ServerConfig.ReserveProcessingDelayMs = ReserveProcessingDelayMs;
            ServerConfig.SyncMode = SyncMode;
            ServerConfig.Storage = Storage;
            ServerConfig.ReplicationEnabled = ReplicationEnabled;
            ServerConfig.PubSubEnabled = PubSubEnabled;
            ServerConfig.ConsensusEnabled = ConsensusEnabled;
            ServerConfig.ClusterNodeCount = ClusterNodeCount;
            ServerConfig.ClusterBasePort = ClusterBasePort;
            ServerConfig.HeartbeatTimeoutMs = HeartbeatTimeoutMs;
            ServerConfig.EventDeliveryAttempts = EventDeliveryAttempts;
            DatabaseConfig.Host = Host;
            DatabaseConfig.Port = DatabasePort;
            DatabaseConfig.Database = Database;
            DatabaseConfig.User = User;
            DatabaseConfig.Password = Password;
            DatabaseConfig.PrimarySchema = PrimarySchema;
            DatabaseConfig.ReplicaSchema = ReplicaSchema;
        }
    }
}
