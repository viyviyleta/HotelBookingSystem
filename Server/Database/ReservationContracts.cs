using Shared.Models;

namespace Server.Database;

public sealed class ReservationCommand
{
    public int RoomId { get; set; }

    public int GuestId { get; set; }

    public DateTime CheckInDate { get; set; }

    public DateTime CheckOutDate { get; set; }

    public bool? UseTransaction { get; set; }

    public bool FailAfterFirstOperation { get; set; }

    public Booking ToBooking() => new()
    {
        RoomId = RoomId,
        GuestId = GuestId,
        CheckInDate = CheckInDate,
        CheckOutDate = CheckOutDate,
        Status = "Created"
    };
}

public enum ReservationStatus
{
    Success,
    RoomNotFound,
    RoomAlreadyReserved,
    DatabaseUnavailable,
    Failed
}


public sealed record ReservationResult(
    ReservationStatus Status,
    string Message,
    Booking? Booking,
    int RoomId,
    bool UsedTransaction,
    bool Committed,
    bool RolledBack,
    int ActiveBookingsForRoomAfter,
    bool RoomAvailableAfter,
    string? DatabaseError,
    IReadOnlyList<string> Steps)
{
     public bool IsPartiallyApplied => ActiveBookingsForRoomAfter > 0 && RoomAvailableAfter;
}

public sealed record OperationOutcome(
    bool IsSuccess,
    string Message,
    bool UsedTransaction,
    bool Committed,
    bool RolledBack,
    string? DatabaseError,
    IReadOnlyList<string> Steps);
