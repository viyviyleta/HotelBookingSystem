using System.Data.Common;
using System.Text.Json;
using Npgsql;
using Server;
using Server.Database;
using Server.Messaging;
using Shared.Network;

namespace Tests;

/// <summary>
/// Общие инструменты тестов слоя БД. Каждый тест создаёт собственную схему в PostgreSQL
/// и удаляет её после проверки, поэтому тесты не видят бронирований друг друга и не
/// мешают ручным прогонам экспериментов в схеме public.
/// </summary>
internal static class TestDatabase
{
    private static string? _currentSchema;

    public static DbBookingService Create(string label, string syncMode = "lock", bool useTransaction = true,
        int raceWindowMs = 60, MessagingHub? hub = null, Server.Consensus.ClusterManager? consensus = null)
    {
        // Тест может создать базу несколько раз: предыдущая схема удаляется сразу, иначе она осталась бы в базе.
        Cleanup();

        DatabaseConfig.ConnectionStringOverride = null;
        _currentSchema = CreateSchema($"riot_lr4_{Slug(label)}_{Guid.NewGuid():N}");

        ServerConfig.SyncMode = syncMode;
        ServerConfig.UseTransactionByDefault = useTransaction;
        ServerConfig.Storage = "postgres";
        ServerConfig.ReserveProcessingDelayMs = 10;
        ServerConfig.RaceWindowMs = raceWindowMs;

        var database = new HotelDatabase();
        database.Initialize();

        // hub - параметр тестов ЛР №6: к сервису подключается тот же механизм событий, что и к серверу.
        // consensus - параметр тестов ЛР №7: запись состояния идёт через голосование узлов.
        return new DbBookingService(new BookingStore(database), hub, consensus);
    }

    /// <summary>Как тест видит своё хранилище: «хост:порт/база (schema …)».</summary>
    public static string Target => DatabaseConfig.Target;

    /// <summary>
    /// Переключает конфигурацию на несуществующую базу данных: подключение невозможно,
    /// и сервер должен честно сообщить об этом вместо выдачи результата.
    /// </summary>
    public static void PointToMissingDatabase() =>
        DatabaseConfig.ConnectionStringOverride =
            $"Host={DatabaseConfig.Host};Port={DatabaseConfig.Port};Database=riot-lr4-missing;Username={DatabaseConfig.User}";

    public static void Cleanup()
    {
        if (_currentSchema is null)
            return;

        string schema = _currentSchema;
        _currentSchema = null;

        DatabaseConfig.ConnectionStringOverride = null;
        DatabaseConfig.Schema = null;

        try
        {
            Execute($"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
        }
        catch (PostgresException)
        {
            // схема осталась в базе — на результат теста не влияет, имя у каждого теста своё
        }
    }

    /// <summary>Схема создаётся до подключения приложения: сам сервер её не плодит.</summary>
    private static string CreateSchema(string schema)
    {
        DatabaseConfig.Schema = null;
        Execute($"CREATE SCHEMA IF NOT EXISTS \"{schema}\"");
        DatabaseConfig.Schema = schema;
        return schema;
    }

    private static void Execute(string sql)
    {
        using DbConnection connection = new NpgsqlConnection(DatabaseConfig.BuildConnectionString());
        connection.Open();

        using DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static string Slug(string label) =>
        new(label.ToLowerInvariant().Where(char.IsLetterOrDigit).ToArray());

    public static string BookingPayload(int roomId, int guestId, int startOffsetDays = 1, int nights = 2,
        bool? useTransaction = null, bool failAfterFirstOperation = false) =>
        JsonSerializer.Serialize(new
        {
            RoomId = roomId,
            GuestId = guestId,
            CheckInDate = DateTime.Today.AddDays(startOffsetDays),
            CheckOutDate = DateTime.Today.AddDays(startOffsetDays + nights),
            UseTransaction = useTransaction,
            FailAfterFirstOperation = failAfterFirstOperation
        });

    public static ReservationReport? ReportOf(Response response) =>
        string.IsNullOrEmpty(response.Data)
            ? null
            : JsonSerializer.Deserialize<ReservationReport>(response.Data,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

    public static string PayloadWithBookingId(int bookingId) =>
        JsonSerializer.Serialize(new { BookingId = bookingId });
}
