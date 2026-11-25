using Npgsql;

namespace Server.Database;

public static class DatabaseConfig
{
    public const string EngineName = "PostgreSQL";

    public const string DefaultDatabase = "hotelbooking";

    public static int LockTimeoutMs { get; set; } = 5000;

    public static string Host { get; set; } = "localhost";
    public static int Port { get; set; } = 5432;
    public static string Database { get; set; } = DefaultDatabase;
    public static string User { get; set; } = Environment.UserName;

    /// <summary>ЛР №8: пароль приходит из переменной окружения контейнера и в журнал не попадает.</summary>
    public static string Password { get; set; } = "";


    public static string? ConnectionStringOverride { get; set; }

  
    public static string? Schema { get; set; }

    public static string? PrimarySchema { get; set; } = "riot_primary";

    public static string? ReplicaSchema { get; set; } = "riot_replica";

    /// <summary>Строкое подключение копии, если её нужно сделать реально недоступной (например, несуществующая база).</summary>
    public static string? PrimaryConnection { get; set; }

    public static string? ReplicaConnection { get; set; }

    public static string BuildConnectionString(string? schema = null, string? connectionOverride = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionOverride ?? ConnectionStringOverride ??
            $"Host={Host};Port={Port};Database={Database};Username={User};Pooling=true");

        if (Password.Length > 0 && string.IsNullOrEmpty(builder.Password))
            builder.Password = Password;

        string? search = schema ?? Schema;

        if (!string.IsNullOrEmpty(search))
            builder.SearchPath = search;

        return builder.ToString();
    }

    public static string Target
    {
        get
        {
            var builder = new NpgsqlConnectionStringBuilder(BuildConnectionString());
            string host = string.IsNullOrEmpty(builder.Host) ? Host : builder.Host;
            string database = string.IsNullOrEmpty(builder.Database) ? Database : builder.Database;
            string target = $"{host}:{builder.Port}/{database}";
            return string.IsNullOrEmpty(builder.SearchPath) ? target : $"{target} (schema {builder.SearchPath})";
        }
    }

    public static string TargetFor(string? schema, string? connectionOverride = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(BuildConnectionString(schema, connectionOverride));
        string host = string.IsNullOrEmpty(builder.Host) ? Host : builder.Host;
        string database = string.IsNullOrEmpty(builder.Database) ? Database : builder.Database;
        string target = $"{host}:{builder.Port}/{database}";
        return string.IsNullOrEmpty(builder.SearchPath) ? target : $"{target} (schema {builder.SearchPath})";
    }

    // ------------------------------------------------------------- Формат значений


    public static string Number(decimal value) =>
        value.ToString(System.Globalization.CultureInfo.InvariantCulture);

    public static DateTime ParseDate(object value) => value switch
    {
        DateTime date => date.Kind == DateTimeKind.Utc ? date : date.ToUniversalTime(),
        string text => ParseDate(text),
        _ => ParseDate(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty)
    };

    public static DateTime ParseDate(string value) =>
        DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out DateTime parsed)
            ? parsed
            : default;

    public static DateTime ToUtcDateTime(object? value) => value switch
    {
        DateTime date => date.Kind == DateTimeKind.Utc ? date : date.ToUniversalTime(),
        string text => ParseDate(text),
        _ => default
    };
}
