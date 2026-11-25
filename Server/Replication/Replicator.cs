using System.Data.Common;
using Npgsql;
using Server.Database;

namespace Server.Replication;


public sealed class Replicator
{
    private const string BookingColumns =
        "id, room_id, guest_id, check_in_date, check_out_date, total_amount, status";

    private const string PaymentColumns = "id, booking_id, amount, payment_date, is_successful";

    // ---------------------------------------------------------------- ИЗМЕНЕНИЕ

    public ReplicationOutcome ReplicateChange(DatabaseInstance source, DatabaseInstance target,
        IReadOnlyList<int> bookingIds, IReadOnlyList<int> roomIds, string clientId)
    {
        string flow = $"{source.Label} → {target.Label}";
        ReplicationLog.Replication(clientId,
            $"START {flow}, changed rows: bookings = [{string.Join(", ", bookingIds)}], " +
            $"rooms = [{string.Join(", ", roomIds)}]");

        try
        {
            ReplicationLog.Replication(clientId, $"UPDATE {target.Label} (schema '{target.Schema}')");

            using DbConnection connection = target.Open();
            int bookings = CopyBookings(connection, source.Schema, target.Schema, bookingIds);
            int payments = CopyPayments(connection, source.Schema, target.Schema, bookingIds);
            int rooms = CopyRooms(connection, source.Schema, target.Schema, bookingIds, roomIds);

            string detail = $"{bookings} booking row(s), {payments} payment row(s), {rooms} room row(s) copied";
            ReplicationLog.Replicated(clientId, source.Label, target.Label, detail);

            return new ReplicationOutcome(true,
                $"Replicated from {source.Label} to {target.Label}: {detail}",
                source.Role, target.Role, bookings, rooms, payments, null);
        }
        catch (Exception ex)
        {
            string error = Describe(ex);
            ReplicationLog.ReplicationFailed(clientId, $"{flow}: {error}");

            return new ReplicationOutcome(false,
                $"Replication FAILED ({flow}): {error}",
                source.Role, target.Role, 0, 0, 0, error);
        }
    }

    // ------------------------------------------------------------ ПОЛНАЯ СИНХРОНИЗАЦИЯ

    public ReplicationOutcome Synchronize(DatabaseInstance source, DatabaseInstance target, string clientId)
    {
        string flow = $"{source.Label} → {target.Label}";
        ReplicationLog.Synchronization(clientId,
            $"START full copy {flow} (source: schema '{source.Schema}', target: schema '{target.Schema}')");

        // Источник читается через его собственный экземпляр: откажённому экземпляру нельзя доверить даже копирование.
        InstanceStatus sourceStatus = source.Inspect();

        if (sourceStatus.State != InstanceState.Available)
        {
            string unavailable = $"{source.Label} (source) is UNAVAILABLE — {sourceStatus.Reason}";
            ReplicationLog.Synchronization(clientId, $"FAIL {flow} — {unavailable}");

            return new ReplicationOutcome(false, $"Synchronization FAILED ({flow}): {unavailable}",
                source.Role, target.Role, 0, 0, 0, unavailable);
        }

        try
        {
            using DbConnection connection = target.Open();
            using DbTransaction transaction = connection.BeginTransaction();

            int roomsBefore = Count(connection, transaction, target.Qualify("rooms"));

            HotelDatabase.ExecuteNonQuery(connection, transaction, $"DELETE FROM {target.Qualify("payments")}");
            HotelDatabase.ExecuteNonQuery(connection, transaction, $"DELETE FROM {target.Qualify("bookings")}");

            int bookings = HotelDatabase.ExecuteNonQuery(connection, transaction,
                $"INSERT INTO {target.Qualify("bookings")} ({BookingColumns}) " +
                $"SELECT {BookingColumns} FROM {source.Qualify("bookings")}");

            int payments = HotelDatabase.ExecuteNonQuery(connection, transaction,
                $"INSERT INTO {target.Qualify("payments")} ({PaymentColumns}) " +
                $"SELECT {PaymentColumns} FROM {source.Qualify("payments")}");

            int rooms = HotelDatabase.ExecuteNonQuery(connection, transaction,
                $"UPDATE {target.Qualify("rooms")} t " +
                "SET is_available = s.is_available, type = s.type, price_per_night = s.price_per_night " +
                $"FROM {source.Qualify("rooms")} s WHERE s.id = t.id");

            ResetSequence(connection, transaction, target, "bookings");
            ResetSequence(connection, transaction, target, "payments");

            transaction.Commit();

            string detail = $"target had {roomsBefore} rooms; copied {bookings} bookings, {payments} payments, " +
                            $"updated {rooms} rooms, sequences realigned";
            ReplicationLog.Synchronization(clientId, $"SUCCESS {flow} — {detail}");

            return new ReplicationOutcome(true, $"Synchronization {flow} finished: {detail}",
                source.Role, target.Role, bookings, rooms, payments, null);
        }
        catch (Exception ex)
        {
            string error = Describe(ex);
            ReplicationLog.Synchronization(clientId, $"FAIL {flow} — {error}");

            return new ReplicationOutcome(false, $"Synchronization FAILED ({flow}): {error}",
                source.Role, target.Role, 0, 0, 0, error);
        }
    }

    // ------------------------------------------------------------------ СВЕРКА

    /// Снимок копии через её собственный экземпляр: если копия помечена недоступной, чтение не удастся.
    public InstanceSnapshot ReadCopy(DatabaseInstance instance, string clientId)
    {
        using DbConnection connection = instance.Open();
        return Snapshot(connection, instance, clientId);
    }

    /// Снимок копии «со стороны»: читает схему напрямую, независимо от состояния, которое установило приложение.
    /// Нужен, чтобы зафиксировать содержимое отказавшего экземпляра в таблице эксперимента.
    public InstanceSnapshot PeekCopy(DatabaseInstance instance)
    {
        var raw = new HotelDatabase();
        using DbConnection connection = raw.Open();
        return Snapshot(connection, instance, "forensics", qualified: true);
    }

    public ConsistencyReport Compare(DatabaseInstance primary, DatabaseInstance replica, string clientId)
    {
        InstanceSnapshot left = PeekCopy(primary);
        InstanceSnapshot right = PeekCopy(replica);

        var differences = new List<string>();

        if (left.Fingerprint != right.Fingerprint)
        {
            differences.Add($"free rooms: Primary = {left.AvailableRooms}/{left.Rooms}, " +
                            $"Replica = {right.AvailableRooms}/{right.Rooms}");
            differences.Add($"bookings: Primary = {left.Bookings} row(s) (active {left.ActiveBookings}), " +
                            $"Replica = {right.Bookings} row(s) (active {right.ActiveBookings})");
            differences.Add($"payments: Primary = {left.Payments}, Replica = {right.Payments}");
            differences.Add("row fingerprints differ");
        }

        string summary = differences.Count == 0
            ? $"Primary = Replica (fingerprint {left.Fingerprint})"
            : $"DIVERGED: {string.Join("; ", differences)}";

        ReplicationLog.Compare(clientId, summary);

        return new ConsistencyReport(differences.Count == 0, summary, differences, left, right);
    }

    private InstanceSnapshot Snapshot(DbConnection connection, DatabaseInstance instance, string clientId,
        bool qualified = false)
    {
        string Prefix(string table) => qualified ? instance.Qualify(table) : table;

        List<(int Id, bool Available, string Type, decimal Price)> rooms = HotelDatabase.ReadList(
            connection, null,
            $"SELECT id, is_available, type, price_per_night FROM {Prefix("rooms")} ORDER BY id",
            reader => (reader.GetInt32(0), Convert.ToBoolean(reader.GetValue(1)), reader.GetString(2),
                HotelDatabase.ToDecimal(reader.GetValue(3))));

        List<(int Id, int RoomId, int GuestId, decimal Total, string Status)> bookings = HotelDatabase.ReadList(
            connection, null,
            $"SELECT id, room_id, guest_id, total_amount, status FROM {Prefix("bookings")} ORDER BY id",
            reader => (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2),
                HotelDatabase.ToDecimal(reader.GetValue(3)), reader.GetString(4)));

        int payments = Convert.ToInt32(HotelDatabase.Scalar(connection, null,
            $"SELECT COUNT(*) FROM {Prefix("payments")}") ?? 0);

        string fingerprint =
            $"rooms[{string.Join("|", rooms.Select(room => $"{room.Id}:{(room.Available ? "TRUE" : "FALSE")}"))}]" +
            $";bookings[{string.Join("|", bookings.Select(b => $"{b.Id}/{b.RoomId}/{b.GuestId}/{DatabaseConfig.Number(b.Total)}/{b.Status}"))}]" +
            $";payments={payments}";

        var snapshot = new InstanceSnapshot(
            instance.Role,
            instance.Schema,
            instance.DatabaseTarget,
            rooms.Count,
            rooms.Count(room => room.Available),
            bookings.Count,
            bookings.Count(b => b.Status != "Cancelled"),
            payments,
            instance.IsAvailable,
            fingerprint);

        ReplicationLog.Snapshot(clientId, instance.Label,
            $"is_available: {string.Join(", ", rooms.Select(room => $"{room.Id} = {(room.Available ? "TRUE" : "FALSE")}"))}, " +
            $"bookings = {snapshot.Bookings} (active = {snapshot.ActiveBookings}), payments = {payments}");

        return snapshot;
    }

    // --------------------------------------------------------------------- SQL

    private static int CopyBookings(DbConnection connection, string? source, string? target,
        IReadOnlyList<int> bookingIds)
    {
        if (bookingIds.Count == 0)
            return 0;

        return HotelDatabase.ExecuteNonQuery(connection, null,
            $"INSERT INTO {target}.bookings ({BookingColumns}) " +
            $"SELECT {BookingColumns} FROM {source}.bookings WHERE id = ANY(@ids) " +
            "ON CONFLICT (id) DO UPDATE SET room_id = EXCLUDED.room_id, guest_id = EXCLUDED.guest_id, " +
            "check_in_date = EXCLUDED.check_in_date, check_out_date = EXCLUDED.check_out_date, " +
            "total_amount = EXCLUDED.total_amount, status = EXCLUDED.status",
            HotelDatabase.Parameter("@ids", bookingIds.ToArray()));
    }

    private static int CopyPayments(DbConnection connection, string? source, string? target,
        IReadOnlyList<int> bookingIds)
    {
        if (bookingIds.Count == 0)
            return 0;

        return HotelDatabase.ExecuteNonQuery(connection, null,
            $"INSERT INTO {target}.payments ({PaymentColumns}) " +
            $"SELECT {PaymentColumns} FROM {source}.payments WHERE booking_id = ANY(@ids) " +
            "ON CONFLICT (id) DO UPDATE SET booking_id = EXCLUDED.booking_id, amount = EXCLUDED.amount, " +
            "payment_date = EXCLUDED.payment_date, is_successful = EXCLUDED.is_successful",
            HotelDatabase.Parameter("@ids", bookingIds.ToArray()));
    }

    private static int CopyRooms(DbConnection connection, string? source, string? target,
        IReadOnlyList<int> bookingIds, IReadOnlyList<int> roomIds)
    {
        if (bookingIds.Count == 0 && roomIds.Count == 0)
            return 0;

        // Отменённая или оплаченная бронь меняет и строку номера, поэтому номер берётся
        // и из списка изменённых, и из броней, которые только что перенесены.
        return HotelDatabase.ExecuteNonQuery(connection, null,
            $"UPDATE {target}.rooms t SET is_available = s.is_available " +
            $"FROM {source}.rooms s WHERE s.id = t.id " +
            "AND (t.id = ANY(@rooms) OR s.id IN (SELECT room_id FROM " +
            $"{source}.bookings WHERE id = ANY(@bookings)))",
            HotelDatabase.Parameter("@rooms", roomIds.ToArray()),
            HotelDatabase.Parameter("@bookings", bookingIds.ToArray()));
    }

    private static void ResetSequence(DbConnection connection, DbTransaction transaction,
        DatabaseInstance target, string table)
    {
        HotelDatabase.Scalar(connection, transaction,
            $"SELECT setval(pg_get_serial_sequence('{target.Qualify(table)}','id'), " +
            $"(SELECT COALESCE(MAX(id), 0) + 1 FROM {target.Qualify(table)}), false)");
    }

    private static int Count(DbConnection connection, DbTransaction transaction, string table) =>
        Convert.ToInt32(HotelDatabase.Scalar(connection, transaction, $"SELECT COUNT(*) FROM {table}") ?? 0);

    private static string Describe(Exception exception)
    {
        string text = exception is PostgresException postgres
            ? $"PostgreSQL error {postgres.SqlState}: {postgres.MessageText}"
            : $"{exception.GetType().Name}: {exception.Message}";

        return string.Join(" ", text.Split('\n', StringSplitOptions.RemoveEmptyEntries |
            StringSplitOptions.TrimEntries));
    }
}
