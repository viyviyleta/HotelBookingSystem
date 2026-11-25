using System.Text.Json;
using Server;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 2 ЛР №4: конкурентный доступ двух и более клиентов к одному номеру,
/// который хранится уже не в памяти, а в таблице rooms базы данных.
/// </summary>
public sealed class DbConcurrencyTests : IDisposable
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public void Dispose() => TestDatabase.Cleanup();

    private static List<ReservationReport> Race(DbBookingService service, int clients, int roomId)
    {
        using var gate = new ManualResetEventSlim(false);

        Task<ReservationReport>[] workers = Enumerable.Range(0, clients).Select(index => Task.Run(async () =>
        {
            // Все клиенты стартуют по одному барьеру, чтобы запросы гарантированно пересекались.
            Request request = RequestBuilderFor(roomId, index);
            gate.Wait();

            Response response = await service.CreateBookingAsync(request.Payload, $"Client {index + 1}");
            return JsonSerializer.Deserialize<ReservationReport>(response.Data, Json)!;
        })).ToArray();

        gate.Set();
        Task.WaitAll(workers);

        return workers.Select(task => task.Result).ToList();
    }

    private static Shared.Network.Request RequestBuilderFor(int roomId, int index) => new()
    {
        Operation = "CreateBooking",
        Payload = TestDatabase.BookingPayload(roomId, guestId: 1 + index % 3)
    };

    private static DatabaseSnapshotReport Snapshot(DbBookingService service) =>
        JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "tester").Data, Json)!;

    [Fact]
    public void Lock_TwoConcurrentClients_ExactlyOneReservesTheRoomInDatabase()
    {
        DbBookingService service = TestDatabase.Create("race-lock", syncMode: "lock");

        List<ReservationReport> reports = Race(service, clients: 2, roomId: 101);

        Assert.Equal(1, reports.Count(report => report.Outcome == "SUCCESS"));
        Assert.Equal(1, reports.Count(report => report.Outcome.StartsWith("FAIL")));

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Single(snapshot.Bookings);
        Assert.False(snapshot.Rooms.Single(room => room.Id == 101).IsAvailable);
        Assert.Equal("consistent", snapshot.ConsistencyWarning);
    }

    [Fact]
    public void WithoutLock_TwoConcurrentClients_SellTheSameRoomTwice()
    {
        DbBookingService service = TestDatabase.Create("race-nolock", syncMode: "none", useTransaction: false);

        List<ReservationReport> reports = Race(service, clients: 2, roomId: 101);

        Assert.Equal(2, reports.Count(report => report.Outcome == "SUCCESS"));

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Equal(2, snapshot.Bookings.Count);
        Assert.Contains("INCONSISTENT", snapshot.ConsistencyWarning);
    }

    [Fact]
    public void Lock_TenConcurrentClients_OnlyOneBookingRowIsWritten()
    {
        DbBookingService service = TestDatabase.Create("race-lock-10", syncMode: "lock", raceWindowMs: 0);

        List<ReservationReport> reports = Race(service, clients: 10, roomId: 101);

        Assert.Equal(1, reports.Count(report => report.Outcome == "SUCCESS"));
        Assert.Single(Snapshot(service).Bookings);
    }

    [Fact]
    public void Lock_DifferentRooms_AreBookedInParallel()
    {
        DbBookingService service = TestDatabase.Create("race-rooms", syncMode: "lock", raceWindowMs: 0);

        Response first = service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1")
            .GetAwaiter().GetResult();
        Response second = service.CreateBookingAsync(TestDatabase.BookingPayload(102, 2), "Client 2")
            .GetAwaiter().GetResult();

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.Equal(2, Snapshot(service).Bookings.Count);
        Assert.Equal(1, Snapshot(service).AvailableRooms);
    }

    /// <summary>
    /// Два эксперимента выше и есть сравнение механизмов: lock не даёт продать один
    /// номер дважды, а транзакция (Tests.TransactionTests) не даёт оставить частичные
    /// изменения при ошибке. Заменить друг друга они не могут.
    /// </summary>
    [Fact]
    public void Lock_KeepsDatabaseInvariant_UnderFiftyConcurrentClients()
    {
        DbBookingService service = TestDatabase.Create("race-lock-50", syncMode: "lock", raceWindowMs: 0);

        Race(service, clients: 50, roomId: 201);

        DatabaseSnapshotReport snapshot = Snapshot(service);
        Assert.Single(snapshot.Bookings);
        Assert.Equal("consistent", snapshot.ConsistencyWarning);
        Assert.Equal(2, snapshot.AvailableRooms);
    }
}
