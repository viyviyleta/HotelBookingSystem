using Shared.Models;
using Shared.Network;

namespace Server;

public class NoSynchronizationBusinessLogic : BusinessLogic
{
    public override string SyncMechanism => "none";

    protected override async Task<Response> ReserveRoomAsync(Booking booking, string clientId)
    {
        var room = DataStorage.FindRoom(booking.RoomId);
        if (room is null)
            return Fail($"Room {booking.RoomId} does not exist");

        ServerLog.Check(clientId, $"room {room.Id}: Available = {room.IsAvailable} (no lock)");

        if (!room.IsAvailable)
        {
            ServerLog.Fail(clientId, "RoomAlreadyReserved");
            return Fail($"Room {room.Id} is already reserved");
        }
        
        await Task.Delay(ServerConfig.ReserveProcessingDelayMs);

        Booking created = ConfirmBooking(room, booking);
        room.IsAvailable = false;

        ServerLog.Success(clientId, $"Booking #{created.Id} for room {room.Id} (oversold risk)");
        ServerLog.Info(clientId, $"→ STATE Available rooms = {DataStorage.Rooms.Count(r => r.IsAvailable)}");

        return new Response
        {
            IsSuccess = true,
            Message = $"Booking #{created.Id} created successfully (mechanism: none)",
            Data = System.Text.Json.JsonSerializer.Serialize(created),
            EventTriggered = "RoomReserved, BookingCreated"
        };
    }
}
