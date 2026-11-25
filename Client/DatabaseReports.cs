namespace Client;


/// Клиентские копии отчётов сервера о работе с базой данных. Клиент не ссылается на
/// типы сервера: он читает тот же JSON, который сервер положил в поле Data ответа.

public sealed record ReservationReport(
    string Outcome,
    string Message,
    int RoomId,
    int BookingId,
    bool UsedTransaction,
    bool Committed,
    bool RolledBack,
    int ActiveBookingsForRoom,
    bool RoomAvailableAfter,
    bool PartialChange,
    string? DatabaseError,
    IReadOnlyList<string> Steps,
    string ServedBy = "database",
    string Replication = "");

public sealed record DatabaseSnapshotReport(
    string Engine,
    string Target,
    List<RoomRow> Rooms,
    List<BookingRow> Bookings,
    List<PaymentRow> Payments,
    int AvailableRooms,
    string ConsistencyWarning,
    string ServedBy = "database");

public sealed record RoomRow(int Id, int HotelId, string RoomNumber, string Type, decimal PricePerNight, bool IsAvailable);

public sealed record BookingRow(int Id, int RoomId, int GuestId, DateTime CheckInDate, DateTime CheckOutDate,
    decimal TotalAmount, string Status);

public sealed record PaymentRow(int Id, int BookingId, decimal Amount, DateTime PaymentDate, bool IsSuccessful);

// --------------------------- Две копии данных и отказоустойчивость (ЛР №5)

public sealed record InstanceStatus(
    string Role,
    string Schema,
    string Target,
    string State,
    string Reason,
    string HealthProbe);

public sealed record CopySnapshot(
    string Role,
    string Schema,
    string Target,
    int Rooms,
    int AvailableRooms,
    int Bookings,
    int ActiveBookings,
    int Payments,
    bool IsAvailable,
    string Fingerprint);

public sealed record ConsistencyReport(
    bool IsConsistent,
    string Summary,
    List<string> Differences,
    CopySnapshot Primary,
    CopySnapshot Replica);

public sealed record ClusterReport(
    bool ReplicationEnabled,
    string Serving,
    string Mode,
    List<InstanceStatus> Instances,
    ConsistencyReport? Consistency,
    string Summary);

public sealed record ReplicationOutcome(
    bool IsSuccess,
    string Message,
    string Source,
    string Target,
    int BookingRows,
    int RoomRows,
    int PaymentRows,
    string? Error);
