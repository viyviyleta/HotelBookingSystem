using System.Text.Json;
using Server;
using ServerSemaphore;
using Shared.Models;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Требования 3, 5, 7 Части 1: сравнение поведения общего ресурса без синхронизации,
/// с lock и с SemaphoreSlim.
/// </summary>
public class SynchronizationTests
{
    private const int RaceDelayMs = 120;

    public SynchronizationTests()
    {
        DataStorage.Reset();
        ServerConfig.ReserveProcessingDelayMs = RaceDelayMs;
    }

    private static string BookingRequest(int roomId = 101)
    {
        var payload = JsonSerializer.Serialize(new Booking
        {
            GuestId = 100,
            RoomId = roomId,
            CheckInDate = DateTime.Today.AddDays(1),
            CheckOutDate = DateTime.Today.AddDays(3)
        });

        return JsonSerializer.Serialize(new Request { Operation = "CreateBooking", Payload = payload });
    }

    private static async Task<List<Response>> ReserveConcurrentlyAsync(RequestHandler handler, int clients)
    {
        string request = BookingRequest();

        Task<string>[] pending = Enumerable.Range(0, clients)
            .Select(i => Task.Run(() => handler.HandleRequestAsync(request, $"Client {i + 1}")))
            .ToArray();

        string[] json = await Task.WhenAll(pending);
        return json.Select(j => JsonSerializer.Deserialize<Response>(j)!).ToList();
    }

    private static int Successes(IEnumerable<Response> responses) => responses.Count(r => r.IsSuccess);

    [Fact]
    public async Task WithoutSynchronization_TwoClientsBothReserveSameRoom_RaceConditionReproduced()
    {
        var handler = new RequestHandler(new NoSynchronizationBusinessLogic());

        List<Response> responses = await ReserveConcurrentlyAsync(handler, clients: 2);

        // Проверка и изменение разделены задержкой → оба клиента видят Available = true
        // и оба получают SUCCESS: один номер продан дважды.
        Assert.Equal(2, Successes(responses));
    }

    [Fact]
    public async Task Lock_TwoClients_OnlyOneReservesTheRoom()
    {
        var handler = new RequestHandler(new LockBusinessLogic());

        List<Response> responses = await ReserveConcurrentlyAsync(handler, clients: 2);

        Assert.Equal(1, Successes(responses));
        Assert.Contains(responses, r => !r.IsSuccess && r.Message.Contains("already reserved"));
        Assert.Single(DataStorage.Bookings);
        Assert.False(DataStorage.FindRoom(101)!.IsAvailable);
    }

    [Fact]
    public async Task Lock_TenClients_ExactlyOneSucceedsAndBookingsMatch()
    {
        var handler = new RequestHandler(new LockBusinessLogic());

        List<Response> responses = await ReserveConcurrentlyAsync(handler, clients: 10);

        Assert.Equal(1, Successes(responses));

        // Общее состояние не испорчено: ровно одна бронировка, номер занят.
        Assert.Single(DataStorage.Bookings);
        Assert.False(DataStorage.FindRoom(101)!.IsAvailable);
    }

    [Fact]
    public async Task SemaphoreSlim_TwoClients_OnlyOneReservesTheRoom()
    {
        var handler = new RequestHandler(new SemaphoreSlimBusinessLogic());

        List<Response> responses = await ReserveConcurrentlyAsync(handler, clients: 2);

        Assert.Equal(1, Successes(responses));
        Assert.Single(DataStorage.Bookings);
    }

    [Fact]
    public async Task SemaphoreSlim_TenClients_ExactlyOneSucceeds()
    {
        var handler = new RequestHandler(new SemaphoreSlimBusinessLogic());

        List<Response> responses = await ReserveConcurrentlyAsync(handler, clients: 10);

        Assert.Equal(1, Successes(responses));
        Assert.Single(DataStorage.Bookings);
    }

    [Fact]
    public async Task Lock_And_SemaphoreSlim_ProduceTheSameOutcome()
    {
        DataStorage.Reset();
        var lockHandler = new RequestHandler(new LockBusinessLogic());
        List<Response> lockResponses = await ReserveConcurrentlyAsync(lockHandler, clients: 4);
        int lockSuccesses = Successes(lockResponses);
        int lockBookings = DataStorage.Bookings.Count;

        DataStorage.Reset();
        var semaphoreHandler = new RequestHandler(new SemaphoreSlimBusinessLogic());
        List<Response> semaphoreResponses = await ReserveConcurrentlyAsync(semaphoreHandler, clients: 4);

        Assert.Equal(1, lockSuccesses);
        Assert.Equal(lockSuccesses, Successes(semaphoreResponses));
        Assert.Equal(lockBookings, DataStorage.Bookings.Count);
    }

    [Fact]
    public async Task ConcurrentCancellations_OnlyOneCancelsAndRoomReleasedOnce()
    {
        var handler = new RequestHandler(new LockBusinessLogic());
        await handler.HandleRequestAsync(BookingRequest(), "Client 1");

        string cancel = JsonSerializer.Serialize(new Request
        {
            Operation = "CancelBooking",
            Payload = JsonSerializer.Serialize(new { BookingId = 1 })
        });

        string[] json = await Task.WhenAll(
            Task.Run(() => handler.HandleRequestAsync(cancel, "Client 1")),
            Task.Run(() => handler.HandleRequestAsync(cancel, "Client 2")));

        var responses = json.Select(j => JsonSerializer.Deserialize<Response>(j)!).ToList();

        Assert.Equal(1, Successes(responses));
        Assert.Contains(responses, r => !r.IsSuccess && r.Message.Contains("already cancelled"));
        Assert.True(DataStorage.FindRoom(101)!.IsAvailable);
    }

    [Fact]
    public async Task ConcurrentPayments_OnlyOnePaymentRecorded()
    {
        var handler = new RequestHandler(new LockBusinessLogic());
        await handler.HandleRequestAsync(BookingRequest(), "Client 1");

        string pay = JsonSerializer.Serialize(new Request
        {
            Operation = "PayBooking",
            Payload = JsonSerializer.Serialize(new { BookingId = 1 })
        });

        string[] json = await Task.WhenAll(
            Task.Run(() => handler.HandleRequestAsync(pay, "Client 1")),
            Task.Run(() => handler.HandleRequestAsync(pay, "Client 2")));

        Assert.Equal(1, json.Select(j => JsonSerializer.Deserialize<Response>(j)!).Count(r => r.IsSuccess));
        Assert.Single(DataStorage.Payments);
    }

    [Fact]
    public async Task BookingIds_AreUniqueUnderConcurrency()
    {
        var handler = new RequestHandler(new LockBusinessLogic());

        // Каждый клиент резервирует свой номер: проверка, что нумерация не «ломается» при параллельности.
        var pending = new[] { 101, 102, 201 }.Select(room => Task.Run(async () =>
        {
            string request = BookingRequest(room);
            return await handler.HandleRequestAsync(request, $"Client {room}");
        }));

        var responses = (await Task.WhenAll(pending))
            .Select(j => JsonSerializer.Deserialize<Response>(j)!)
            .ToList();

        Assert.Equal(3, Successes(responses));

        List<int> ids = DataStorage.Bookings.Select(b => b.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }
}
