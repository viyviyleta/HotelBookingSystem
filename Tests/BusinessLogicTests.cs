using System.Text.Json;
using Xunit;
using Server;
using Shared.Models;
using Shared.Network;

namespace Tests;

/// <summary>
/// Операции предметной области «Бронирование отелей» (вариант 2) на сервере с lock.
/// </summary>
public class BusinessLogicTests
{
    private readonly RequestHandler _handler = new(new LockBusinessLogic());

    public BusinessLogicTests()
    {
        DataStorage.Reset();
        ServerConfig.ReserveProcessingDelayMs = 10;
    }

    private async Task<Response> SendAsync(string operation, string payload, string clientId = "TestClient")
    {
        string json = await _handler.HandleRequestAsync(
            JsonSerializer.Serialize(new Request { Operation = operation, Payload = payload }),
            clientId);

        return JsonSerializer.Deserialize<Response>(json)!;
    }

    private static string BookingPayload(int roomId = 101, int inDays = 1, int outDays = 5) =>
        JsonSerializer.Serialize(new Booking
        {
            RoomId = roomId,
            GuestId = 1,
            CheckInDate = DateTime.Today.AddDays(inDays),
            CheckOutDate = DateTime.Today.AddDays(outDays)
        });

    [Fact]
    public async Task CreateBooking_ValidData_ShouldSucceed()
    {
        Response response = await SendAsync("CreateBooking", BookingPayload());

        Assert.True(response.IsSuccess);
        Assert.Equal("RoomReserved, BookingCreated", response.EventTriggered);

        Booking? booking = JsonSerializer.Deserialize<Booking>(response.Data);
        Assert.NotNull(booking);
        Assert.Equal(6 * 100m, booking.TotalAmount);
        Assert.Equal("Created", booking.Status);
    }

    [Fact]
    public async Task CreateBooking_TwoClientsSameRoom_SecondIsRejected()
    {
        Response first = await SendAsync("CreateBooking", BookingPayload(), "Client 1");
        Response second = await SendAsync("CreateBooking", BookingPayload(), "Client 2");

        Assert.True(first.IsSuccess);
        Assert.False(second.IsSuccess);
        Assert.Contains("already reserved", second.Message);
    }

    [Theory]
    [InlineData(-1, 2)]
    [InlineData(1, 1)]
    [InlineData(3, 2)]
    public async Task CreateBooking_InvalidDates_ShouldBeRejected(int inDays, int outDays)
    {
        Response response = await SendAsync("CreateBooking", BookingPayload(inDays: inDays, outDays: outDays));

        Assert.False(response.IsSuccess);
    }

    [Fact]
    public async Task CreateBooking_UnknownRoom_ShouldBeRejected()
    {
        Response response = await SendAsync("CreateBooking", BookingPayload(roomId: 777));

        Assert.False(response.IsSuccess);
        Assert.Contains("does not exist", response.Message);
    }

    [Fact]
    public async Task CheckAvailability_ReturnsOnlyFreeRooms()
    {
        Response before = await SendAsync("CheckAvailability", "{}");
        var roomsBefore = JsonSerializer.Deserialize<List<Room>>(before.Data)!;

        await SendAsync("CreateBooking", BookingPayload(roomId: 101));

        Response after = await SendAsync("CheckAvailability", "{}");
        var roomsAfter = JsonSerializer.Deserialize<List<Room>>(after.Data)!;

        Assert.Equal(3, roomsBefore.Count);
        Assert.Equal(2, roomsAfter.Count);
        Assert.DoesNotContain(roomsAfter, r => r.Id == 101);
    }

    [Fact]
    public async Task PayBooking_AfterCreate_ShouldSucceedOnce()
    {
        await SendAsync("CreateBooking", BookingPayload());

        Response paid = await SendAsync("PayBooking", """{"BookingId":1}""");
        Response repeated = await SendAsync("PayBooking", """{"BookingId":1}""");

        Assert.True(paid.IsSuccess);
        Assert.Equal("PaymentCompleted", paid.EventTriggered);
        Assert.False(repeated.IsSuccess);
        Assert.Contains("already paid", repeated.Message);
    }

    [Fact]
    public async Task CancelBooking_ReleasesRoomAndCanBeBookedAgain()
    {
        await SendAsync("CreateBooking", BookingPayload());
        Response cancelled = await SendAsync("CancelBooking", """{"BookingId":1}""");
        Response repeated = await SendAsync("CancelBooking", """{"BookingId":1}""");
        Response bookedAgain = await SendAsync("CreateBooking", BookingPayload());

        Assert.True(cancelled.IsSuccess);
        Assert.Equal("BookingCancelled", cancelled.EventTriggered);
        Assert.False(repeated.IsSuccess);
        Assert.True(bookedAgain.IsSuccess);
    }

    [Fact]
    public async Task PayBooking_MissingBookingId_ShouldBeRejected()
    {
        Response noField = await SendAsync("PayBooking", "{}");
        Response notNumber = await SendAsync("PayBooking", """{"BookingId":"abc"}""");
        Response brokenJson = await SendAsync("PayBooking", "{ broken");

        Assert.False(noField.IsSuccess);
        Assert.False(notNumber.IsSuccess);
        Assert.False(brokenJson.IsSuccess);
    }

    [Fact]
    public async Task BookingAndPaymentNumbers_DoNotRepeatAfterResets()
    {
        await SendAsync("CreateBooking", BookingPayload());
        await SendAsync("CancelBooking", """{"BookingId":1}""");
        Response second = await SendAsync("CreateBooking", BookingPayload());

        Booking? booking = JsonSerializer.Deserialize<Booking>(second.Data)!;
        Assert.Equal(2, booking.Id);
    }
}
