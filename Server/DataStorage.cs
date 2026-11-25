using Shared.Models;

namespace Server;


public static class DataStorage
{

    public static readonly object SyncRoot = new();

    public static List<Hotel> Hotels { get; } = new()
    {
        new Hotel { Id = 1, Name = "Grand Hotel", City = "Minsk" },
        new Hotel { Id = 2, Name = "Plaza", City = "Brest" }
    };

    public static List<Room> Rooms { get; } = new()
    {
        new Room { Id = 101, HotelId = 1, RoomNumber = "101", Type = "Single", PricePerNight = 100, IsAvailable = true },
        new Room { Id = 102, HotelId = 1, RoomNumber = "102", Type = "Double", PricePerNight = 150, IsAvailable = true },
        new Room { Id = 201, HotelId = 2, RoomNumber = "201", Type = "Suite", PricePerNight = 300, IsAvailable = true }
    };

    public static List<Booking> Bookings { get; } = new();
    public static List<Payment> Payments { get; } = new();


    private static int _nextBookingId = 1;
    private static int _nextPaymentId = 1;

    public static int NextBookingId() => _nextBookingId++;

    public static int NextPaymentId() => _nextPaymentId++;

    public static Room? FindRoom(int roomId) => Rooms.FirstOrDefault(r => r.Id == roomId);

    public static Booking? FindBooking(int bookingId) => Bookings.FirstOrDefault(b => b.Id == bookingId);

    public static void Reset()
    {
        lock (SyncRoot)
        {
            Bookings.Clear();
            Payments.Clear();
            foreach (var room in Rooms)
                room.IsAvailable = true;

            _nextBookingId = 1;
            _nextPaymentId = 1;
        }
    }
}
