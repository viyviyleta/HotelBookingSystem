using Server;
using Server.Replication;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 4 ЛР №5: восстановление Primary, синхронизация копий и возврат к штатному маршруту
/// без перезапуска сервера.
/// </summary>
public sealed class RecoveryTests : IDisposable
{
    public void Dispose()
    {
        TestCluster.Cleanup();
        TestDatabase.Cleanup();
    }

    /// <summary>Штатная для этого раздела последовательность: отказ Primary, запись в Replica, восстановление Primary.</summary>
    private static async Task<(DbBookingService Service, int BookingId)> OutageWithWriteAsync(string label)
    {
        DbBookingService service = TestCluster.Create(label);

        service.FailInstance(TestCluster.RolePayload("primary"), "Controller");
        int bookingId = TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!.BookingId;
        service.RestoreInstance(TestCluster.RolePayload("primary"), "Controller");

        return (service, bookingId);
    }

    // --------------------------------------------- Требование 4: восстановление Primary

    [Fact]
    public void RestoreInstance_MakesPrimaryAvailableAgain()
    {
        DbBookingService service = TestCluster.Create("restore");
        service.FailInstance(TestCluster.RolePayload("primary"), "Controller");

        Response response = service.RestoreInstance(TestCluster.RolePayload("primary"), "Client 1");
        var status = TestCluster.Data<InstanceStatus>(response);

        Assert.True(response.IsSuccess);
        Assert.Equal(InstanceState.Available, status.State);
        Assert.Contains("restored by Client 1", status.Reason);
        Assert.Contains("connected, rooms = 3", status.HealthProbe);
    }

    [Fact]
    public void Route_ReturnsToPrimaryAfterRestore_WithoutRestartingTheServer()
    {
        DbBookingService service = TestCluster.Create("failback");
        InstanceCluster cluster = service.Cluster!;

        cluster.Primary.Fail("primary is down");
        Assert.Same(cluster.Replica, cluster.Route("Client 1"));

        cluster.Primary.Restore("primary is back");

        Assert.Same(cluster.Primary, cluster.Route("Client 2"));
    }

    // ------------------------------------------------------- Расхождение после восстановления

    [Fact]
    public async Task CompareReplicas_ListsChangesMadeOnlyOnReplica()
    {
        (DbBookingService service, _) = await OutageWithWriteAsync("diverged");

        Response response = service.CompareReplicas("{}", "Client 1");
        var report = TestCluster.Data<ConsistencyReport>(response);

        Assert.False(report.IsConsistent);
        Assert.Equal("ReplicasDiverged", response.EventTriggered);
        Assert.Contains(report.Differences,
            difference => difference.Contains("bookings: Primary = 0") && difference.Contains("Replica = 1"));
        Assert.Contains("DIVERGED", report.Summary);
    }

    // ------------------------- Требование 4: синхронизация после восстановления и совпадение копий

    [Fact]
    public async Task SynchronizeReplicas_ReplicaToPrimary_RecoversChangesMadeDuringOutage()
    {
        (DbBookingService service, _) = await OutageWithWriteAsync("sync-back");

        Response response = service.SynchronizeReplicas(TestCluster.SyncPayload("replica", "primary"), "Client 1");
        var outcome = TestCluster.Data<ReplicationOutcome>(response);

        Assert.True(response.IsSuccess);
        Assert.Equal("SynchronizationDone", response.EventTriggered);
        Assert.Equal(InstanceRole.Replica, outcome.Source);
        Assert.Equal(1, outcome.BookingRows);

        ConsistencyReport consistency = TestCluster.Compare(service);
        Assert.True(consistency.IsConsistent);
        Assert.Equal(consistency.Primary.Fingerprint, consistency.Replica.Fingerprint);

        InstanceSnapshot primary = TestCluster.ReadOf(service, InstanceRole.Primary);
        Assert.Equal(1, primary.Bookings);
        Assert.Equal(2, primary.AvailableRooms);
    }

    [Fact]
    public async Task ControlOperation_AfterSynchronization_IsServedByPrimaryAndReplicated()
    {
        (DbBookingService service, int bookingId) = await OutageWithWriteAsync("after-sync");
        service.SynchronizeReplicas(TestCluster.SyncPayload("replica", "primary"), "Client 1");

        Response cancel = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");
        var report = TestCluster.Data<ChangeReport>(cancel);

        Assert.True(cancel.IsSuccess);
        Assert.Equal("Primary", report.ServedBy);
        Assert.Contains("REPLICATION Primary → Replica: OK", report.Replication);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    // -------------------------------------------------- Требование 8: невозможная синхронизация

    [Fact]
    public void SynchronizeReplicas_SameSourceAndTarget_IsRejected()
    {
        DbBookingService service = TestCluster.Create("sync-self");

        Response response = service.SynchronizeReplicas(TestCluster.SyncPayload("primary", "primary"), "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Contains("source and target must differ", response.Message);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public void SynchronizeReplicas_FromUnavailableSource_IsRejected()
    {
        DbBookingService service = TestCluster.Create("sync-dead-source");
        service.Cluster!.Replica.Fail("replica is down for the synchronization test");

        Response response = service.SynchronizeReplicas(TestCluster.SyncPayload("replica", "primary"), "Client 1");
        var outcome = TestCluster.Data<ReplicationOutcome>(response);

        Assert.False(response.IsSuccess);
        Assert.Equal("SynchronizationFailed", response.EventTriggered);
        Assert.Contains("Replica (source) is UNAVAILABLE", outcome.Error);
    }

    [Fact]
    public void SynchronizeReplicas_InvalidRole_IsRejected()
    {
        DbBookingService service = TestCluster.Create("sync-bad-role");

        Response response = service.SynchronizeReplicas(TestCluster.SyncPayload("primary", "standby"), "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Contains("must be 'primary' or 'replica'", response.Message);
    }

    [Fact]
    public void SynchronizeReplicas_FromMissingDatabaseCopy_ReportsRealConnectionError()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("sync-3d000");
        service.Cluster!.Replica.Inspect();

        Response response = service.SynchronizeReplicas(TestCluster.SyncPayload("replica", "primary"), "Client 1");
        var outcome = TestCluster.Data<ReplicationOutcome>(response);

        Assert.False(response.IsSuccess);
        Assert.Contains("connection error detected by health check", outcome.Error);
    }
}
