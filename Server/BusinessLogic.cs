using System.Text.Json;
using Shared.Models;
using Shared.Network;

namespace Server;

public abstract class BusinessLogic : IBookingApi
{
    public abstract string SyncMechanism { get; }

    protected object SyncRoot => DataStorage.SyncRoot;

    public async Task<Response> CheckAvailabilityAsync(string payload, string clientId)
    {
        ServerLog.Start(clientId, "CheckAvailability");

        await Task.Delay(ServerConfig.ReserveProcessingDelayMs);

        List<Room> availableRooms;
        string mechanism = SyncMechanism;
        lock (SyncRoot)
        {
            // Синхронизация чтения: защита коллекции от изменения другим потоком во время перечисления
            availableRooms = DataStorage.Rooms.Where(r => r.IsAvailable).Select(r => r.Clone()).ToList();
        }

        ServerLog.Check(clientId, $"Available rooms = {availableRooms.Count} (mechanism: {mechanism})");
        ServerLog.Success(clientId, "AvailableRooms");

        return new Response
        {
            IsSuccess = true,
            Message = "Available rooms retrieved",
            Data = JsonSerializer.Serialize(availableRooms)
        };
    }

    public async Task<Response> CreateBookingAsync(string payload, string clientId)
    {
        Booking? booking;
        try
        {
            booking = JsonSerializer.Deserialize<Booking>(payload);
        }
        catch (JsonException ex)
        {
            ServerLog.Fail(clientId, $"Invalid booking payload: {ex.Message}");
            return Fail("Invalid payload: booking data is not valid JSON");
        }

        if (booking is null)
        {
            ServerLog.Fail(clientId, "Empty booking payload");
            return Fail("Invalid payload: booking data is missing");
        }

        if (booking.CheckInDate == default || booking.CheckOutDate == default)
            return Fail("Invalid data: check-in and check-out dates are required");

        if (booking.CheckInDate.Date < DateTime.Today)
            return Fail("Check-in date cannot be in the past");

        if (booking.CheckOutDate <= booking.CheckInDate)
            return Fail("Check-out date must be after check-in date");

        ServerLog.Start(clientId, $"Reserve (room {booking.RoomId})");

        // Искусственная задержка вне критической секции для проверки параллельной обработки
        await Task.Delay(ServerConfig.ReserveProcessingDelayMs);

        return await ReserveRoomAsync(booking, clientId);
    }

    /// (lock / SemaphoreSlim / без синхронизации)
    protected abstract Task<Response> ReserveRoomAsync(Booking booking, string clientId);

    public Response PayBooking(string payload, string clientId)
    {
        ServerLog.Start(clientId, "PayBooking");

        if (!TryReadBookingId(payload, out int bookingId))
        {
            ServerLog.Fail(clientId, "BookingId is missing or invalid");
            return Fail("BookingId is required and must be an integer");
        }

        Payment payment;
        lock (SyncRoot)
        {
            ServerLog.Lock(clientId);

            var booking = DataStorage.FindBooking(bookingId);
            if (booking is null)
            {
                ServerLog.Fail(clientId, $"Booking #{bookingId} does not exist");
                ServerLog.Unlock(clientId);
                return Fail("Booking not found");
            }

            if (booking.Status == "Paid")
            {
                ServerLog.Check(clientId, $"Booking #{bookingId} already paid");
                ServerLog.Fail(clientId, "Double payment");
                ServerLog.Unlock(clientId);
                return Fail("Booking is already paid");
            }

            booking.Status = "Paid";
            payment = new Payment
            {
                Id = DataStorage.NextPaymentId(),
                BookingId = booking.Id,
                Amount = booking.TotalAmount,
                PaymentDate = DateTime.UtcNow,
                IsSuccessful = true
            };
            DataStorage.Payments.Add(payment);

            ServerLog.Success(clientId, $"Payment #{payment.Id} for Booking #{booking.Id}");
            ServerLog.Unlock(clientId);
        }

        return new Response
        {
            IsSuccess = true,
            Message = $"Payment for Booking #{bookingId} completed successfully",
            Data = JsonSerializer.Serialize(payment),
            EventTriggered = "PaymentCompleted"
        };
    }

    public Response CancelBooking(string payload, string clientId)
    {
        ServerLog.Start(clientId, "CancelBooking");

        if (!TryReadBookingId(payload, out int bookingId))
        {
            ServerLog.Fail(clientId, "BookingId is missing or invalid");
            return Fail("BookingId is required and must be an integer");
        }

        lock (SyncRoot)
        {
            ServerLog.Lock(clientId);

            var booking = DataStorage.FindBooking(bookingId);
            if (booking is null)
            {
                ServerLog.Fail(clientId, $"Booking #{bookingId} does not exist");
                ServerLog.Unlock(clientId);
                return Fail("Booking not found");
            }

            if (booking.Status == "Cancelled")
            {
                ServerLog.Check(clientId, $"Booking #{bookingId} already cancelled");
                ServerLog.Fail(clientId, "Double cancellation");
                ServerLog.Unlock(clientId);
                return Fail("Booking is already cancelled");
            }

            booking.Status = "Cancelled";

            var room = DataStorage.FindRoom(booking.RoomId);
            if (room is not null)
            {
                room.IsAvailable = true;
                ServerLog.Check(clientId, $"Room {room.Id} released");
            }

            ServerLog.Success(clientId, $"Booking #{bookingId} cancelled");
            ServerLog.Unlock(clientId);
        }

        return new Response
        {
            IsSuccess = true,
            Message = $"Booking #{bookingId} cancelled successfully",
            EventTriggered = "BookingCancelled"
        };
    }

    protected static Response Fail(string message) =>
        new() { IsSuccess = false, Message = message };

    protected static bool TryReadBookingId(string payload, out int bookingId)
    {
        bookingId = 0;
        try
        {
            using var doc = JsonDocument.Parse(payload);
            if (!doc.RootElement.TryGetProperty("BookingId", out var idElem))
                return false;

            return idElem.TryGetInt32(out bookingId);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    protected static Booking ConfirmBooking(Room room, Booking booking)
    {
        int days = (booking.CheckOutDate - booking.CheckInDate).Days;
        booking.Id = DataStorage.NextBookingId();
        booking.TotalAmount = days * room.PricePerNight;
        booking.Status = "Created";
        DataStorage.Bookings.Add(booking);
        return booking;
    }
}