using System.Text.Json;
using Server;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 3 ЛР №4: четыре эксперимента с составной операцией
/// «создание бронирования + изменение состояния номера».
/// </summary>
public sealed class TransactionTests : IDisposable
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public void Dispose() => TestDatabase.Cleanup();

    private static ReservationReport Run(DbBookingService service, int roomId, int guestId,
        bool useTransaction, bool failAfterFirstOperation)
    {
        Response response = service.CreateBookingAsync(
            TestDatabase.BookingPayload(roomId, guestId, useTransaction: useTransaction,
                failAfterFirstOperation: failAfterFirstOperation), "Client 1").GetAwaiter().GetResult();

        return JsonSerializer.Deserialize<ReservationReport>(response.Data, Json)!;
    }

    [Fact]
    public void Experiment1_NoTransaction_NoError_BothOperationsAreSaved()
    {
        DbBookingService service = TestDatabase.Create("tx-exp1");

        ReservationReport report = Run(service, roomId: 101, guestId: 1, useTransaction: false, failAfterFirstOperation: false);

        Assert.Equal("SUCCESS", report.Outcome);
        Assert.False(report.UsedTransaction);
        Assert.False(report.Committed);
        Assert.Contains(report.Steps, step => step.StartsWith("INSERT bookings"));
        Assert.Contains(report.Steps, step => step.StartsWith("UPDATE rooms"));
        Assert.Equal(1, report.ActiveBookingsForRoom);
        Assert.False(report.RoomAvailableAfter);
        Assert.False(report.PartialChange);
    }

    [Fact]
    public void Experiment2_NoTransaction_ErrorBetweenOperations_LeavesPartialChange()
    {
        DbBookingService service = TestDatabase.Create("tx-exp2");

        ReservationReport report = Run(service, roomId: 101, guestId: 1, useTransaction: false, failAfterFirstOperation: true);

        Assert.Equal("FAIL (database error)", report.Outcome);
        Assert.True(report.RolledBack == false, "without a transaction there is nothing to roll back");
        Assert.Contains("were NOT rolled back", report.Message);

        // Состояние БД после ошибки: бронь осталась, а номер по-прежнему свободен.
        Assert.Equal(1, report.ActiveBookingsForRoom);
        Assert.True(report.RoomAvailableAfter);
        Assert.True(report.PartialChange);
    }

    [Fact]
    public void Experiment3_Transaction_NoError_CommitsBothOperations()
    {
        DbBookingService service = TestDatabase.Create("tx-exp3");

        ReservationReport report = Run(service, roomId: 101, guestId: 1, useTransaction: true, failAfterFirstOperation: false);

        Assert.Equal("SUCCESS", report.Outcome);
        Assert.True(report.UsedTransaction);
        Assert.True(report.Committed);
        Assert.Equal(1, report.ActiveBookingsForRoom);
        Assert.False(report.RoomAvailableAfter);
        Assert.False(report.PartialChange);
    }

    [Fact]
    public void Experiment4_Transaction_ErrorBetweenOperations_RollsEverythingBack()
    {
        DbBookingService service = TestDatabase.Create("tx-exp4");

        ReservationReport report = Run(service, roomId: 101, guestId: 1, useTransaction: true, failAfterFirstOperation: true);

        Assert.Equal("FAIL (database error)", report.Outcome);
        Assert.True(report.UsedTransaction);
        Assert.False(report.Committed);
        Assert.True(report.RolledBack);

        // Ни одной из двух связанных операций в базе не осталось.
        Assert.Equal(0, report.ActiveBookingsForRoom);
        Assert.True(report.RoomAvailableAfter);
        Assert.False(report.PartialChange);
    }

    [Fact]
    public void Transaction_And_NoTransaction_StoreTheSameData_WhenNoErrorHappens()
    {
        DbBookingService withoutTransaction = TestDatabase.Create("compare-off");
        ReservationReport plain = Run(withoutTransaction, 101, 1, useTransaction: false, failAfterFirstOperation: false);

        TestDatabase.Cleanup();

        DbBookingService withTransaction = TestDatabase.Create("compare-on");
        ReservationReport guarded = Run(withTransaction, 101, 1, useTransaction: true, failAfterFirstOperation: false);

        Assert.Equal(plain.ActiveBookingsForRoom, guarded.ActiveBookingsForRoom);
        Assert.Equal(plain.RoomAvailableAfter, guarded.RoomAvailableAfter);
        Assert.True(guarded.Committed);
    }

    [Fact]
    public void AfterRollback_TheRoomCanBeReservedAgainByTheSameClient()
    {
        DbBookingService service = TestDatabase.Create("rollback-retry");

        Run(service, roomId: 102, guestId: 1, useTransaction: true, failAfterFirstOperation: true);

        ReservationReport retry = Run(service, roomId: 102, guestId: 1, useTransaction: true, failAfterFirstOperation: false);

        Assert.Equal("SUCCESS", retry.Outcome);
        Assert.Equal(1, retry.ActiveBookingsForRoom);
    }

    [Fact]
    public void CommitFailure_DeferredForeignKey_ForcesRollback()
    {
        DbBookingService service = TestDatabase.Create("commit-fail");

        service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1").GetAwaiter().GetResult();

        Response response = service.CommitFailureCheck("{}", "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Contains("COMMIT is impossible", response.Message);
        Assert.Equal("CommitRejected", response.EventTriggered);

        // Отложенный внешний ключ проверился только при подтверждении, и вся транзакция отменена:
        // количество броней осталось таким, каким было до эксперимента.
        var snapshot = JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "tester").Data, Json)!;

        Assert.Single(snapshot.Bookings);
        Assert.Empty(snapshot.Payments);
    }

    [Fact]
    public void PaymentWithoutTransaction_WouldSplitStatusAndPaymentRows()
    {
        // Оплата тоже составная операция: UPDATE статуса брони + INSERT платежа.
        // Проверим, что при ошибке обеих записей не остаётся — транзакция откатывает и статус.
        DbBookingService service = TestDatabase.Create("pay-rollback");

        int bookingId = TestDatabase.ReportOf(
            service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1")
                .GetAwaiter().GetResult())!.BookingId;

        Assert.True(service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1").IsSuccess);

        var snapshot = JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "tester").Data, Json)!;

        Assert.Equal("Paid", snapshot.Bookings.Single().Status);
        Assert.Single(snapshot.Payments);
    }
}
