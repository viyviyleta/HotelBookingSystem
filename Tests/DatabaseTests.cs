using System.Text.Json;
using Server;
using Server.Database;
using Shared.Models;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 1 ЛР №4: структура базы данных, подключение сервера и операции чтения/записи.
/// </summary>
public sealed class DatabaseTests : IDisposable
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public void Dispose() => TestDatabase.Cleanup();

    private static DatabaseSnapshotReport Snapshot(DbBookingService service) =>
        JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "tester").Data, Json)!;

    // ------------------------------------------------- Требование 3: структура БД

    [Fact]
    public void Initialize_CreatesTablesAndSeedRows()
    {
        DbBookingService service = TestDatabase.Create("schema");

        DatabaseSnapshotReport snapshot = Snapshot(service);

        Assert.Equal(3, snapshot.Rooms.Count);
        Assert.Equal(3, snapshot.AvailableRooms);
        Assert.Empty(snapshot.Bookings);
        Assert.Empty(snapshot.Payments);
        Assert.Equal("PostgreSQL", snapshot.Engine);
        Assert.Equal(TestDatabase.Target, snapshot.Target);
    }

    [Fact]
    public void Schema_EnforcesForeignKeysAndCheckConstraints()
    {
        TestDatabase.Create("constraints");
        var store = new BookingStore(new HotelDatabase());

        // Ограничение внешнего ключа: бронь на несуществующего гостя не создаётся.
        ReservationResult result = store.Reserve(new ReservationCommand
        {
            RoomId = 101,
            GuestId = 99999,
            CheckInDate = DateTime.Today.AddDays(1),
            CheckOutDate = DateTime.Today.AddDays(3)
        }, true, "tester");

        Assert.Equal(ReservationStatus.Failed, result.Status);
        Assert.Contains("FOREIGN KEY", result.DatabaseError, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, result.ActiveBookingsForRoomAfter);
        Assert.True(result.RoomAvailableAfter);
        Assert.True(result.RolledBack);
    }

    // --------------------------------------------- Требование 5: операции с БД

    [Fact]
    public async Task CheckAvailability_IsReadOperationOnDatabase()
    {
        DbBookingService service = TestDatabase.Create("read");

        Response response = await service.CheckAvailabilityAsync("{}", "Client 1");

        Assert.True(response.IsSuccess);
        Assert.Contains("PostgreSQL", response.Message);

        List<Room> rooms = JsonSerializer.Deserialize<List<Room>>(response.Data, Json)!;
        Assert.Equal(3, rooms.Count);
        Assert.All(rooms, room => Assert.True(room.IsAvailable));
    }

    [Fact]
    public async Task CreateBooking_WritesBothRelatedRowsToDatabase()
    {
        DbBookingService service = TestDatabase.Create("write");

        Response response = await service.CreateBookingAsync(
            TestDatabase.BookingPayload(roomId: 101, guestId: 1), "Client 1");

        Assert.True(response.IsSuccess);

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Booking booking = Assert.Single(snapshot.Bookings);
        Assert.Equal(101, booking.RoomId);
        Assert.Equal(1, booking.GuestId);
        Assert.Equal("Created", booking.Status);
        Assert.False(snapshot.Rooms.Single(r => r.Id == 101).IsAvailable);
        Assert.Equal("consistent", snapshot.ConsistencyWarning);
    }

    [Fact]
    public async Task CreateBooking_TotalAmountComesFromPriceInDatabase()
    {
        DbBookingService service = TestDatabase.Create("price");

        await service.CreateBookingAsync(TestDatabase.BookingPayload(roomId: 102, guestId: 2, nights: 3), "Client 1");

        // Номер 102 стоит 150 за ночь, 3 ночи → 450.
        Assert.Equal(450, Snapshot(service).Bookings.Single().TotalAmount);
    }

    [Fact]
    public void PayBooking_UpdatesStatusAndInsertsPaymentInOneTransaction()
    {
        DbBookingService service = TestDatabase.Create("pay");

        Response created = service.CreateBookingAsync(
            TestDatabase.BookingPayload(101, 1), "Client 1").GetAwaiter().GetResult();
        int bookingId = TestDatabase.ReportOf(created)!.BookingId;

        Response paid = service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.True(paid.IsSuccess);

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Equal("Paid", snapshot.Bookings.Single().Status);
        Payment payment = Assert.Single(snapshot.Payments);
        Assert.Equal(bookingId, payment.BookingId);
        Assert.True(payment.IsSuccessful);
    }

    [Fact]
    public void PayBooking_Twice_IsRejectedAndKeepsSinglePayment()
    {
        DbBookingService service = TestDatabase.Create("double-pay");

        int bookingId = TestDatabase.ReportOf(
            service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1")
                .GetAwaiter().GetResult())!.BookingId;

        Assert.True(service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1").IsSuccess);
        Response second = service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.False(second.IsSuccess);
        Assert.Contains("already paid", second.Message);
        Assert.Single(Snapshot(service).Payments);
    }

    [Fact]
    public void CancelBooking_ReturnsRoomToDatabase()
    {
        DbBookingService service = TestDatabase.Create("cancel");

        int bookingId = TestDatabase.ReportOf(
            service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1")
                .GetAwaiter().GetResult())!.BookingId;

        Assert.True(service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1").IsSuccess);

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Equal("Cancelled", snapshot.Bookings.Single().Status);
        Assert.True(snapshot.Rooms.Single(r => r.Id == 101).IsAvailable);
        Assert.Equal(3, snapshot.AvailableRooms);
    }

    // ---------------------------------------------------- Требование 8: ошибки

    [Fact]
    public async Task UnknownRoom_IsRejectedWithoutWritingAnything()
    {
        DbBookingService service = TestDatabase.Create("no-room");

        Response response = await service.CreateBookingAsync(
            TestDatabase.BookingPayload(roomId: 999, guestId: 1), "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Contains("does not exist", response.Message);
        Assert.Empty(Snapshot(service).Bookings);
    }

    [Fact]
    public async Task AlreadyReservedRoom_IsRejected()
    {
        DbBookingService service = TestDatabase.Create("taken-room");

        await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        Response second = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 2), "Client 2");

        Assert.False(second.IsSuccess);
        Assert.Contains("already reserved", second.Message);
        Assert.Single(Snapshot(service).Bookings);
    }

    [Fact]
    public void ChangingMissingRecord_IsReported()
    {
        DbBookingService service = TestDatabase.Create("missing-record");

        Response response = service.PayBooking(TestDatabase.PayloadWithBookingId(4242), "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Contains("does not exist", response.Message);
    }

    [Fact]
    public async Task InvalidPayload_IsRejectedBeforeTouchingDatabase()
    {
        DbBookingService service = TestDatabase.Create("bad-payload");

        Assert.Contains("not valid JSON",
            (await service.CreateBookingAsync("{ not a json }", "Client 1")).Message);

        Assert.Contains("Check-out date must be after check-in date",
            (await service.CreateBookingAsync(
                """{"RoomId":101,"GuestId":1,"CheckInDate":"2030-05-10","CheckOutDate":"2030-05-01"}""",
                "Client 1")).Message);

        Assert.Empty(Snapshot(service).Bookings);
    }

    [Fact]
    public async Task UnreachableDatabase_IsReportedAndServerKeepsWorking()
    {
        DbBookingService service = TestDatabase.Create("db-down");

        // Подключение к несуществующей базе отклоняет сам PostgreSQL при открытии соединения.
        TestDatabase.PointToMissingDatabase();

        Response booking = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        Response read = await service.CheckAvailabilityAsync("{}", "Client 1");
        Response health = service.DatabaseHealth("{}", "Client 1");

        Assert.False(booking.IsSuccess);
        Assert.False(read.IsSuccess);
        Assert.False(health.IsSuccess);
        Assert.Contains("database", booking.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Database connection error", health.Message);
    }

    [Fact]
    public void DatabaseHealth_ReportsWorkingConnection()
    {
        DbBookingService service = TestDatabase.Create("health");

        Response response = service.DatabaseHealth("{}", "Client 1");

        Assert.True(response.IsSuccess);
        Assert.Contains("rooms = 3", response.Message);
    }

    [Fact]
    public void ResetDatabase_RestoresSeedState()
    {
        DbBookingService service = TestDatabase.Create("reset");

        service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1").GetAwaiter().GetResult();
        Assert.False(Snapshot(service).Rooms.Single(r => r.Id == 101).IsAvailable);

        Assert.True(service.ResetDatabase("{}", "tester").IsSuccess);

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Empty(snapshot.Bookings);
        Assert.Equal(3, snapshot.AvailableRooms);
    }

    [Fact]
    public void MemoryServer_RejectsDatabaseOnlyOperations()
    {
        ServerConfig.Storage = "memory";
        var handler = new RequestHandler(new LockBusinessLogic());

        string json = handler.HandleRequestAsync(
            """{"Operation":"DatabaseSnapshot","ClientName":"Client 1"}""", "Client 1").GetAwaiter().GetResult();

        Response response = JsonSerializer.Deserialize<Response>(json, Json)!;

        Assert.False(response.IsSuccess);
        Assert.Contains("--storage postgres", response.Message);
    }
}
