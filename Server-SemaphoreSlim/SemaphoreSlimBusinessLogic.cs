using Server;
using Shared.Models;
using Shared.Network;

namespace ServerSemaphore;


public class SemaphoreSlimBusinessLogic : BusinessLogic
{

    private static readonly SemaphoreSlim Gate = new(1, 1);

    private static int _waiting;

    public override string SyncMechanism => "SemaphoreSlim";

    protected override async Task<Response> ReserveRoomAsync(Booking booking, string clientId)
    {
        int queued = Interlocked.Increment(ref _waiting);
        ServerLog.Info(clientId, $"→ [WAIT] clients in queue = {queued}");
        await Gate.WaitAsync();
        Interlocked.Decrement(ref _waiting);

        try
        {
            ServerLog.Lock(clientId);

            var room = DataStorage.FindRoom(booking.RoomId);
            if (room is null)
            {
                ServerLog.Check(clientId, $"room {booking.RoomId} = not found");
                ServerLog.Fail(clientId, "NoSuchRoom");
                return Fail($"Room {booking.RoomId} does not exist");
            }

            ServerLog.Check(clientId, $"room {room.Id}: Available = {room.IsAvailable}");

            if (!room.IsAvailable)
            {
                ServerLog.Fail(clientId, "RoomAlreadyReserved");
                return Fail($"Room {room.Id} is already reserved");
            }

            room.IsAvailable = false;
            Booking created = ConfirmBooking(room, booking);

            await Task.Yield();

            ServerLog.Success(clientId, $"Booking #{created.Id} for room {room.Id}, total = {created.TotalAmount}");
            ServerLog.Info(clientId, $"→ STATE Available rooms = {DataStorage.Rooms.Count(r => r.IsAvailable)}");

            return new Response
            {
                IsSuccess = true,
                Message = $"Booking #{created.Id} created successfully (mechanism: SemaphoreSlim)",
                Data = System.Text.Json.JsonSerializer.Serialize(created),
                EventTriggered = "RoomReserved, BookingCreated"
            };
        }
        finally
        {
            Gate.Release();
            ServerLog.Unlock(clientId);
        }
    }
}
