using Shared.Models;
using Shared.Network;

namespace Server;

public class LockBusinessLogic : BusinessLogic
{
    public override string SyncMechanism => "lock";

    protected override Task<Response> ReserveRoomAsync(Booking booking, string clientId)
    {
        Response response;

        lock (SyncRoot)
        {
            ServerLog.Lock(clientId);

            var room = DataStorage.FindRoom(booking.RoomId);
            if (room is null)
            {
                ServerLog.Check(clientId, $"room {booking.RoomId} = not found");
                ServerLog.Fail(clientId, "NoSuchRoom");
                ServerLog.Unlock(clientId);
                return Task.FromResult(Fail($"Room {booking.RoomId} does not exist"));
            }

            ServerLog.Check(clientId, $"room {room.Id}: Available = {room.IsAvailable}");

            if (!room.IsAvailable)
            {
                ServerLog.Fail(clientId, "RoomAlreadyReserved");
                ServerLog.Unlock(clientId);
                return Task.FromResult(Fail($"Room {room.Id} is already reserved"));
            }

            room.IsAvailable = false;
            Booking created = ConfirmBooking(room, booking);

            ServerLog.Success(clientId, $"Booking #{created.Id} for room {room.Id}, total = {created.TotalAmount}");
            ServerLog.Info(clientId, $"→ STATE Available rooms = {DataStorage.Rooms.Count(r => r.IsAvailable)}");

            response = new Response
            {
                IsSuccess = true,
                Message = $"Booking #{created.Id} created successfully (mechanism: lock)",
                Data = System.Text.Json.JsonSerializer.Serialize(created),
                EventTriggered = "RoomReserved, BookingCreated"
            };
        }

        ServerLog.Unlock(clientId);
        return Task.FromResult(response);
    }
}
