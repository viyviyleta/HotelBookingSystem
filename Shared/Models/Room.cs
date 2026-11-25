namespace Shared.Models;

public class Room
{
    public int Id { get; set; }
    public int HotelId { get; set; }
    public string RoomNumber { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty; // Single, Double
    public decimal PricePerNight { get; set; }
    public bool IsAvailable { get; set; } = true;

    /// <summary>Снимок состояния для выдачи клиенту, чтобы не сериализовать общий объект вне критической секции.</summary>
    public Room Clone() => new()
    {
        Id = Id,
        HotelId = HotelId,
        RoomNumber = RoomNumber,
        Type = Type,
        PricePerNight = PricePerNight,
        IsAvailable = IsAvailable
    };
}
