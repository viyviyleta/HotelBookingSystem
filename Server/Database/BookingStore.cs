using System.Data.Common;
using Npgsql;
using Shared.Models;

namespace Server.Database;

public sealed class BookingStore
{
    private const string RoomColumns =
        "SELECT id, hotel_id, room_number, type, price_per_night, is_available FROM rooms";

    private const string BookingColumns =
        "SELECT id, room_id, guest_id, check_in_date, check_out_date, total_amount, status FROM bookings";

    private readonly HotelDatabase _database;

    public BookingStore(HotelDatabase database) => _database = database;

    public HotelDatabase Database => _database;

    // ------------------------------------------------------------------ ЧТЕНИЕ

    public List<Room> GetRooms()
    {
        using DbConnection connection = _database.Open();
        return ReadRooms(connection, null);
    }

    public List<Booking> GetBookings()
    {
        using DbConnection connection = _database.Open();
        return HotelDatabase.ReadList(connection, null, BookingColumns + " ORDER BY id", ReadBooking);
    }

    public List<Payment> GetPayments()
    {
        using DbConnection connection = _database.Open();
        return HotelDatabase.ReadList(connection, null,
            "SELECT id, booking_id, amount, payment_date, is_successful FROM payments ORDER BY id", ReadPayment);
    }

    /// <summary>Бронь целиком: из неё берутся данные события предметной области (ЛР №6).</summary>
    public Booking? ReadBookingById(int bookingId)
    {
        using DbConnection connection = _database.Open();
        return HotelDatabase.ReadList(connection, null, BookingColumns + " WHERE id = @id", ReadBooking,
            HotelDatabase.Parameter("@id", bookingId)).FirstOrDefault();
    }

    /// Фактическое состояние общих данных, прочитанное независимым запросом
    /// уже после COMMIT/ROLLBACK — используется для проверки результата эксперимента.

    public (int ActiveBookings, bool RoomAvailable) ReadRoomState(int roomId)
    {
        using DbConnection connection = _database.Open();

        int bookings = Convert.ToInt32(HotelDatabase.Scalar(connection, null,
            "SELECT COUNT(*) FROM bookings WHERE room_id = @id AND status <> 'Cancelled'",
            HotelDatabase.Parameter("@id", roomId)) ?? 0);

        object? available = HotelDatabase.Scalar(connection, null,
            "SELECT is_available FROM rooms WHERE id = @id",
            HotelDatabase.Parameter("@id", roomId));

        return (bookings, available is null || Convert.ToBoolean(available));
    }

    // ------------------------------------------- СОСТАВНАЯ ОПЕРАЦИЯ (Часть 3)

    public ReservationResult Reserve(ReservationCommand command, bool defaultUseTransaction, string clientId,
        int raceWindowMs = 0)
    {
        bool useTransaction = command.UseTransaction ?? defaultUseTransaction;
        var steps = new List<string>();

        DatabaseLog.Mode(clientId, useTransaction
            ? "WITH transaction — BEGIN / COMMIT / ROLLBACK"
            : "WITHOUT transaction — each statement is committed immediately (autocommit)");

        DbConnection connection;
        try
        {
            connection = _database.Open();
        }
        catch (DatabaseConnectionException ex)
        {
            DatabaseLog.Error(clientId, ex.Message);
            return new ReservationResult(ReservationStatus.DatabaseUnavailable,
                "Database is unavailable, reservation rejected", null, command.RoomId,
                useTransaction, false, false, 0, true, ex.Message, steps);
        }

        using (connection)
        {
            DbTransaction? transaction = null;
            ReservationStatus status = ReservationStatus.Failed;
            Booking? created = null;
            string? databaseError = null;
            string message = string.Empty;
            bool committed = false;
            bool rolledBack = false;

            try
            {
                if (useTransaction)
                {
                    transaction = connection.BeginTransaction();
                    DatabaseLog.Begin(clientId);
                }

                Room? room = FindRoom(connection, transaction, command.RoomId);
                steps.Add($"SELECT rooms WHERE id = {command.RoomId}");
                DatabaseLog.Statement(clientId, $"SELECT rooms WHERE id = {command.RoomId}",
                    room is null
                        ? "no row"
                        : $"is_available = {Flag(room.IsAvailable)}, price_per_night = {DatabaseConfig.Number(room.PricePerNight)}");

                if (room is null)
                {
                    status = ReservationStatus.RoomNotFound;
                    message = $"Room {command.RoomId} does not exist";
                    DatabaseLog.Check(clientId, $"room {command.RoomId}: no such row in table rooms");
                }
                else if (!room.IsAvailable)
                {
                    status = ReservationStatus.RoomAlreadyReserved;
                    message = $"Room {command.RoomId} is already reserved";
                    DatabaseLog.Check(clientId, $"room {command.RoomId}: is_available = FALSE → reservation refused");
                }
                else
                {
                    DatabaseLog.Check(clientId, $"room {command.RoomId}: is_available = TRUE → reservation allowed");

                    if (raceWindowMs > 0)
                    {
                        DatabaseLog.State(clientId,
                            $"race window: {raceWindowMs} ms between the check and the write, lock is not held");
                        Thread.Sleep(raceWindowMs);
                    }

                    created = ReserveBothOperations(connection, transaction, command, room, useTransaction, steps, clientId);
                    status = ReservationStatus.Success;
                    message = $"Booking #{created.Id} created and room {created.RoomId} reserved";
                }

                if (transaction is not null)
                {
                    if (status == ReservationStatus.Success)
                    {
                        transaction.Commit();
                        committed = true;
                        DatabaseLog.Commit(clientId);
                    }
                    else
                    {
                        transaction.Rollback();
                        rolledBack = true;
                        DatabaseLog.Rollback(clientId, message);
                    }
                }
            }
            catch (Exception ex)
            {
                status = ReservationStatus.Failed;
                databaseError = DescribeError(ex);
                DatabaseLog.Error(clientId, databaseError);

                if (transaction is not null && TryRollback(transaction, clientId, ex.Message, out bool didRollback))
                    rolledBack = didRollback;
                else if (!useTransaction)
                    DatabaseLog.State(clientId,
                        "ROLLBACK is impossible without a transaction — Operation 1 stays in the database");

                message = ex is InjectedOperationException
                    ? $"Composite operation aborted: {ex.Message}. Changes " +
                      (useTransaction ? "were rolled back." : "were NOT rolled back (no transaction).")
                    : databaseError;
            }
            finally
            {
                transaction?.Dispose();
            }

            (int activeBookings, bool roomAvailable) = ReadRoomState(command.RoomId);
            DatabaseLog.State(clientId,
                $"room {command.RoomId}: active bookings = {activeBookings}, is_available = {Flag(roomAvailable)}" +
                (activeBookings > 0 && roomAvailable ? "  ← INCONSISTENT (partial change)" : string.Empty));

            return new ReservationResult(status, message, created, command.RoomId, useTransaction,
                committed, rolledBack, activeBookings, roomAvailable, databaseError, steps);
        }
    }

    private static Booking ReserveBothOperations(DbConnection connection, DbTransaction? transaction,
        ReservationCommand command, Room room, bool useTransaction, List<string> steps, string clientId)
    {
        var booking = command.ToBooking();
        int days = (booking.CheckOutDate - booking.CheckInDate).Days;
        booking.TotalAmount = days * room.PricePerNight;

        object? newId = HotelDatabase.Scalar(connection, transaction,
            """
            INSERT INTO bookings (room_id, guest_id, check_in_date, check_out_date, total_amount, status)
            VALUES (@roomId, @guestId, @checkIn, @checkOut, @total, 'Created')
            RETURNING id
            """,
            HotelDatabase.Parameter("@roomId", booking.RoomId),
            HotelDatabase.Parameter("@guestId", booking.GuestId),
            HotelDatabase.Parameter("@checkIn", booking.CheckInDate),
            HotelDatabase.Parameter("@checkOut", booking.CheckOutDate),
            HotelDatabase.Parameter("@total", booking.TotalAmount));

        booking.Id = Convert.ToInt32(newId);
        steps.Add($"INSERT bookings (room {booking.RoomId}, guest {booking.GuestId}) → id = {booking.Id}");
        DatabaseLog.Operation(clientId, 1,
            $"INSERT bookings (room_id = {booking.RoomId}, guest_id = {booking.GuestId}, total = {DatabaseConfig.Number(booking.TotalAmount)})",
            $"SUCCESS, generated booking id = {booking.Id}");

        if (!useTransaction)
            DatabaseLog.Autocommit(clientId, $"INSERT bookings id = {booking.Id}");

        if (command.FailAfterFirstOperation)
            throw new InjectedOperationException(
                $"simulated application error after Operation 1 (booking {booking.Id}) and before Operation 2");

        int rows = HotelDatabase.ExecuteNonQuery(connection, transaction,
            "UPDATE rooms SET is_available = @available WHERE id = @roomId",
            HotelDatabase.Parameter("@available", false),
            HotelDatabase.Parameter("@roomId", room.Id));

        if (rows == 0)
            throw new InjectedOperationException(
                $"UPDATE rooms SET is_available = FALSE WHERE id = {room.Id} affected 0 rows — record does not exist");

        steps.Add($"UPDATE rooms SET is_available = FALSE WHERE id = {room.Id} → {rows} row");
        DatabaseLog.Operation(clientId, 2,
            $"UPDATE rooms SET is_available = FALSE WHERE id = {room.Id}",
            $"SUCCESS, {rows} row updated");

        if (!useTransaction)
            DatabaseLog.Autocommit(clientId, $"UPDATE rooms id = {room.Id}");

        return booking;
    }


    /// Демонстрация «невозможность выполнить COMMIT»: внешний ключ payments.booking_id
    /// объявлен DEFERRABLE INITIALLY DEFERRED, поэтому обе операции внутри транзакции
    /// проходят, а нарушение ограничения возникает только в момент подтверждения.
 
    public OperationOutcome SimulateCommitFailure(string clientId)
    {
        var steps = new List<string>();

        using DbConnection connection = _database.Open();

        int bookingsBefore = CountRows(connection, "bookings");
        int paymentsBefore = CountRows(connection, "payments");

        string? commitError = null;
        bool rolledBack = false;

        using (DbTransaction transaction = connection.BeginTransaction())
        {
            DatabaseLog.Begin(clientId);

            try
            {
                object? bookingId = HotelDatabase.Scalar(connection, transaction,
                    """
                    INSERT INTO bookings (room_id, guest_id, check_in_date, check_out_date, total_amount, status)
                    VALUES (101, 1, @checkIn, @checkOut, 200, 'Created')
                    RETURNING id
                    """,
                    HotelDatabase.Parameter("@checkIn", DateTime.UtcNow.AddDays(5)),
                    HotelDatabase.Parameter("@checkOut", DateTime.UtcNow.AddDays(7)));

                int createdBooking = Convert.ToInt32(bookingId);
                steps.Add($"INSERT bookings → id = {createdBooking}");
                DatabaseLog.Operation(clientId, 1, "INSERT bookings", $"SUCCESS, id = {createdBooking}");

                HotelDatabase.ExecuteNonQuery(connection, transaction,
                    """
                    INSERT INTO payments (booking_id, amount, payment_date, is_successful)
                    VALUES (@bookingId, 100, @date, @successful)
                    """,
                    HotelDatabase.Parameter("@bookingId", 999999),
                    HotelDatabase.Parameter("@date", DateTime.UtcNow),
                    HotelDatabase.Parameter("@successful", true));

                steps.Add("INSERT payments (booking_id = 999999 — no such record)");
                DatabaseLog.Operation(clientId, 2, "INSERT payments (booking_id = 999999)",
                    "SUCCESS inside transaction — the foreign key check is deferred until COMMIT");

                transaction.Commit();

                return new OperationOutcome(false,
                    "Unexpected: COMMIT succeeded, so the deferred foreign key was not checked",
                    true, true, false, null, steps);
            }
            catch (DbException ex)
            {
                commitError = DescribeError(ex);
                DatabaseLog.Error(clientId, $"COMMIT failed → {commitError}");
                steps.Add($"COMMIT -> FAILED ({ex.Message})");

                rolledBack = TryRollback(transaction, clientId,
                    "COMMIT rejected by the deferred foreign key", out bool didRollback) && didRollback;
            }
        }

        int bookingsAfter = CountRows(connection, "bookings");
        int paymentsAfter = CountRows(connection, "payments");

        string state = bookingsAfter == bookingsBefore && paymentsAfter == paymentsBefore
            ? $"database unchanged (bookings {bookingsBefore} → {bookingsAfter}, payments {paymentsBefore} → {paymentsAfter})"
            : $"DATABASE CHANGED (bookings {bookingsBefore} → {bookingsAfter}, payments {paymentsBefore} → {paymentsAfter})";

        DatabaseLog.State(clientId, state);

        return new OperationOutcome(false,
            $"COMMIT is impossible: {commitError}. After ROLLBACK the {state}.",
            true, false, rolledBack, commitError, steps);
    }

    private static int CountRows(DbConnection connection, string table) =>
        Convert.ToInt32(HotelDatabase.Scalar(connection, null, $"SELECT COUNT(*) FROM {table}") ?? 0);

    // ------------------------------------------------- ПРОЧИЕ ОПЕРАЦИИ ЗАПИСИ

    public OperationOutcome PayBooking(int bookingId, string clientId)
    {
        var steps = new List<string>();

        using DbConnection connection = _database.Open();
        using DbTransaction transaction = connection.BeginTransaction();
        DatabaseLog.Begin(clientId);

        try
        {
            Booking? booking = FindBooking(connection, transaction, bookingId);
            if (booking is null)
            {
                transaction.Rollback();
                DatabaseLog.Rollback(clientId, $"booking {bookingId} not found");
                return new OperationOutcome(false, $"Booking {bookingId} does not exist", true, false, true, null, steps);
            }

            if (booking.Status == "Paid")
            {
                transaction.Rollback();
                DatabaseLog.Check(clientId, $"booking {bookingId}: status already 'Paid'");
                return new OperationOutcome(false, "Booking is already paid", true, false, true, null, steps);
            }

            int rows = HotelDatabase.ExecuteNonQuery(connection, transaction,
                "UPDATE bookings SET status = 'Paid' WHERE id = @id AND status <> 'Paid'",
                HotelDatabase.Parameter("@id", bookingId));

            if (rows == 0)
                throw new InjectedOperationException($"UPDATE bookings WHERE id = {bookingId} affected 0 rows");

            steps.Add($"UPDATE bookings SET status = 'Paid' → {rows} row");
            DatabaseLog.Operation(clientId, 1, $"UPDATE bookings SET status = 'Paid' WHERE id = {bookingId}",
                $"SUCCESS, {rows} row updated");

            object? paymentId = HotelDatabase.Scalar(connection, transaction,
                """
                INSERT INTO payments (booking_id, amount, payment_date, is_successful)
                VALUES (@bookingId, @amount, @date, @successful)
                RETURNING id
                """,
                HotelDatabase.Parameter("@bookingId", bookingId),
                HotelDatabase.Parameter("@amount", booking.TotalAmount),
                HotelDatabase.Parameter("@date", DateTime.UtcNow),
                HotelDatabase.Parameter("@successful", true));

            steps.Add($"INSERT payments → id = {paymentId}");
            DatabaseLog.Operation(clientId, 2, "INSERT payments", $"SUCCESS, payment id = {paymentId}");

            transaction.Commit();
            DatabaseLog.Commit(clientId);

            return new OperationOutcome(true,
                $"Payment #{paymentId} for booking #{bookingId} saved (amount = {DatabaseConfig.Number(booking.TotalAmount)})",
                true, true, false, null, steps);
        }
        catch (Exception ex)
        {
            TryRollback(transaction, clientId, ex.Message, out bool rolledBack);
            DatabaseLog.Error(clientId, DescribeError(ex));
            return new OperationOutcome(false, DescribeError(ex), true, false, rolledBack, ex.Message, steps);
        }
    }

    public OperationOutcome CancelBooking(int bookingId, string clientId)
    {
        var steps = new List<string>();

        using DbConnection connection = _database.Open();
        using DbTransaction transaction = connection.BeginTransaction();
        DatabaseLog.Begin(clientId);

        try
        {
            Booking? booking = FindBooking(connection, transaction, bookingId);
            if (booking is null)
            {
                transaction.Rollback();
                return new OperationOutcome(false, $"Booking {bookingId} does not exist", true, false, true, null, steps);
            }

            if (booking.Status == "Cancelled")
            {
                transaction.Rollback();
                DatabaseLog.Check(clientId, $"booking {bookingId}: status already 'Cancelled'");
                return new OperationOutcome(false, "Booking is already cancelled", true, false, true, null, steps);
            }

            int statusRows = HotelDatabase.ExecuteNonQuery(connection, transaction,
                "UPDATE bookings SET status = 'Cancelled' WHERE id = @id",
                HotelDatabase.Parameter("@id", bookingId));

            int roomRows = HotelDatabase.ExecuteNonQuery(connection, transaction,
                "UPDATE rooms SET is_available = @available WHERE id = (SELECT room_id FROM bookings WHERE id = @id)",
                HotelDatabase.Parameter("@available", true),
                HotelDatabase.Parameter("@id", bookingId));

            if (statusRows == 0 || roomRows == 0)
                throw new InjectedOperationException(
                    $"affected rows: bookings = {statusRows}, rooms = {roomRows} — expected 1 and 1");

            steps.Add($"UPDATE bookings → {statusRows} row, UPDATE rooms → {roomRows} row");
            transaction.Commit();
            DatabaseLog.Commit(clientId);

            return new OperationOutcome(true,
                $"Booking #{bookingId} cancelled and room {booking.RoomId} released", true, true, false, null, steps);
        }
        catch (Exception ex)
        {
            TryRollback(transaction, clientId, ex.Message, out bool rolledBack);
            DatabaseLog.Error(clientId, DescribeError(ex));
            return new OperationOutcome(false, DescribeError(ex), true, false, rolledBack, ex.Message, steps);
        }
    }

    public void Reset()
    {
        using DbConnection connection = _database.Open();
        using DbTransaction transaction = connection.BeginTransaction();

        HotelDatabase.ExecuteNonQuery(connection, transaction, "DELETE FROM payments");
        HotelDatabase.ExecuteNonQuery(connection, transaction, "DELETE FROM bookings");
        HotelDatabase.ExecuteNonQuery(connection, transaction, "UPDATE rooms SET is_available = @available",
            HotelDatabase.Parameter("@available", true));

        transaction.Commit();
    }

    // ------------------------------------------------------------------ СЛУЖЕБНОЕ

    private static string Flag(bool value) => value ? "TRUE" : "FALSE";


    private static string DescribeError(Exception exception)
    {
        string text = exception is PostgresException postgres
            ? $"PostgreSQL error {postgres.SqlState}: {postgres.MessageText}"
            : $"{exception.GetType().Name}: {exception.Message}";

        return string.Join(" ", text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
    }

    private static bool TryRollback(DbTransaction transaction, string clientId, string reason, out bool rolledBack)
    {
        try
        {
            transaction.Rollback();
            DatabaseLog.Rollback(clientId, reason);
            rolledBack = true;
            return true;
        }
        catch (InvalidOperationException)
        {
            DatabaseLog.State(clientId, "transaction already finished — nothing to roll back");
            rolledBack = false;
            return false;
        }
        catch (DbException ex)
        {
            DatabaseLog.Error(clientId, $"ROLLBACK itself failed: {ex.Message}");
            rolledBack = false;
            return false;
        }
    }

    private static Room? FindRoom(DbConnection connection, DbTransaction? transaction, int roomId) =>
        HotelDatabase.ReadList(connection, transaction, RoomColumns + " WHERE id = @id", ReadRoom,
            HotelDatabase.Parameter("@id", roomId)).FirstOrDefault();

    private static Booking? FindBooking(DbConnection connection, DbTransaction? transaction, int bookingId) =>
        HotelDatabase.ReadList(connection, transaction, BookingColumns + " WHERE id = @id", ReadBooking,
            HotelDatabase.Parameter("@id", bookingId)).FirstOrDefault();

    private static List<Room> ReadRooms(DbConnection connection, DbTransaction? transaction) =>
        HotelDatabase.ReadList(connection, transaction, RoomColumns + " ORDER BY id", ReadRoom);

    private static Room ReadRoom(DbDataReader reader) => new()
    {
        Id = reader.GetInt32(0),
        HotelId = reader.GetInt32(1),
        RoomNumber = reader.GetString(2),
        Type = reader.GetString(3),
        PricePerNight = HotelDatabase.ToDecimal(reader.GetValue(4)),
        IsAvailable = Convert.ToBoolean(reader.GetValue(5))
    };

    private static Booking ReadBooking(DbDataReader reader) => new()
    {
        Id = reader.GetInt32(0),
        RoomId = reader.GetInt32(1),
        GuestId = reader.GetInt32(2),
        CheckInDate = DatabaseConfig.ParseDate(reader.GetValue(3)),
        CheckOutDate = DatabaseConfig.ParseDate(reader.GetValue(4)),
        TotalAmount = HotelDatabase.ToDecimal(reader.GetValue(5)),
        Status = reader.GetString(6)
    };

    private static Payment ReadPayment(DbDataReader reader) => new()
    {
        Id = reader.GetInt32(0),
        BookingId = reader.GetInt32(1),
        Amount = HotelDatabase.ToDecimal(reader.GetValue(2)),
        PaymentDate = DatabaseConfig.ParseDate(reader.GetValue(3)),
        IsSuccessful = Convert.ToBoolean(reader.GetValue(4))
    };
}
