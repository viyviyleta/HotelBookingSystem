using System.Data.Common;
using Npgsql;

namespace Server.Database;


public class HotelDatabase
{
    private static readonly System.Text.RegularExpressions.Regex Identifier =
        new("^[a-z][a-z0-9_]*$", System.Text.RegularExpressions.RegexOptions.Compiled);

    private readonly string? _schema;

    public HotelDatabase(string? schema = null) => _schema = schema;

    public string? SchemaName => _schema;

    public virtual string DatabaseTarget => DatabaseConfig.TargetFor(_schema);

    protected virtual string ConnectionString => DatabaseConfig.BuildConnectionString(_schema);

    public static bool IsSafeIdentifier(string value) => Identifier.IsMatch(value);

    public virtual DbConnection Open() => Open(ConnectionString, DatabaseTarget);

    private static DbConnection Open(string connectionString, string target)
    {
        var connection = new NpgsqlConnection(connectionString);

        try
        {
            connection.Open();


            using DbCommand command = connection.CreateCommand();
            command.CommandText = $"SET lock_timeout TO '{DatabaseConfig.LockTimeoutMs}'";
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            connection.Dispose();
            throw new DatabaseConnectionException(
                $"cannot open {DatabaseConfig.EngineName} database '{target}': {ex.Message}", ex);
        }

        return connection;
    }

    /// <summary>Схема экземпляра создаётся заранее: подключению к несуществующей схеме PostgreSQL мешает search_path.</summary>
    public void EnsureSchema()
    {
        if (string.IsNullOrEmpty(_schema))
            return;

        if (!IsSafeIdentifier(_schema))
            throw new ArgumentException($"schema name '{_schema}' is not a safe PostgreSQL identifier", nameof(_schema));

        var withoutSearchPath = new NpgsqlConnectionStringBuilder(ConnectionString) { SearchPath = null };

        using DbConnection connection = Open(withoutSearchPath.ToString(), DatabaseConfig.Target);
        ExecuteNonQuery(connection, null, $"CREATE SCHEMA IF NOT EXISTS {_schema}");
    }

    public void Initialize()
    {
        EnsureSchema();

        using DbConnection connection = Open();

        foreach (string statement in Split(DatabaseInitializer.Schema))
            ExecuteNonQuery(connection, null, statement);

        foreach (string statement in Split(DatabaseInitializer.Seed))
            ExecuteNonQuery(connection, null, statement);
    }

    public bool TryProbe(out string error)
    {
        try
        {
            using DbConnection connection = Open();
            object? count = Scalar(connection, null, "SELECT COUNT(*) FROM rooms");
            error = $"ok, rooms = {count}";
            return true;
        }
        catch (Exception ex)
        {
            error = $"{ex.GetType().Name}: {ex.Message}";
            return false;
        }
    }

    public static object? Scalar(DbConnection connection, DbTransaction? transaction, string sql,
        params DbParameter[] parameters)
    {
        using DbCommand command = Command(connection, transaction, sql, parameters);
        object? value = command.ExecuteScalar();
        return value is DBNull ? null : value;
    }

    public static int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string sql,
        params DbParameter[] parameters)
    {
        using DbCommand command = Command(connection, transaction, sql, parameters);
        return command.ExecuteNonQuery();
    }

    public static List<T> ReadList<T>(DbConnection connection, DbTransaction? transaction, string sql,
        Func<DbDataReader, T> map, params DbParameter[] parameters)
    {
        using DbCommand command = Command(connection, transaction, sql, parameters);
        using DbDataReader reader = command.ExecuteReader();

        var results = new List<T>();
        while (reader.Read())
            results.Add(map(reader));

        return results;
    }

    /// Транзакцию нужно указывать явно: драйвер проверяет принадлежность команды ей.
    private static DbCommand Command(DbConnection connection, DbTransaction? transaction, string sql,
        DbParameter[] parameters)
    {
        DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;

        foreach (DbParameter parameter in parameters)
            command.Parameters.Add(parameter);

        return command;
    }

    public static DbParameter Parameter(string name, object? value) => new NpgsqlParameter(name, value switch
    {
        null => DBNull.Value,
        DateTime date => date.ToUniversalTime(),
        _ => value
    });

    private static IEnumerable<string> Split(string script) =>
        script.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    public static decimal ToDecimal(object value) => Convert.ToDecimal(value);
}
