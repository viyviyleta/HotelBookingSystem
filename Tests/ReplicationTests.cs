using Server;
using Server.Replication;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Части 1 и 2 ЛР №5: начальный кластер «Primary + Replica» и репликация изменений общего ресурса.
/// </summary>
public sealed class ReplicationTests : IDisposable
{
    public void Dispose()
    {
        TestCluster.Cleanup();
        TestDatabase.Cleanup();
    }

    // -------------------------------------------------- Требование 4: начальное состояние

    [Fact]
    public void Initialize_CreatesTwoIndependentCopiesWithTheSameData()
    {
        DbBookingService service = TestCluster.Create("two-copies");
        InstanceCluster cluster = service.Cluster!;

        Assert.Equal(TestCluster.PrimarySchema, cluster.Primary.Schema);
        Assert.Equal(TestCluster.ReplicaSchema, cluster.Replica.Schema);
        Assert.NotEqual(cluster.Primary.DatabaseTarget, cluster.Replica.DatabaseTarget);
        Assert.All(cluster.Status(), status => Assert.Equal(InstanceState.Available, status.State));

        InstanceSnapshot replica = TestCluster.ReadOf(service, InstanceRole.Replica);
        Assert.Equal(3, replica.Rooms);
        Assert.Equal(3, replica.AvailableRooms);
        Assert.Equal(0, replica.Bookings);
        Assert.Equal(0, replica.Payments);

        ConsistencyReport consistency = TestCluster.Compare(service);
        Assert.True(consistency.IsConsistent);
        Assert.Equal(consistency.Primary.Fingerprint, consistency.Replica.Fingerprint);
    }

    [Fact]
    public void InstanceStatus_ReportsBothCopiesAndServingPrimary()
    {
        DbBookingService service = TestCluster.Create("status");

        Response response = service.InstanceStatus("{}", "Client 1");
        var report = TestCluster.Data<ClusterReport>(response);

        Assert.True(response.IsSuccess);
        Assert.True(report.ReplicationEnabled);
        Assert.Equal("Primary", report.Serving);
        Assert.Equal(2, report.Instances.Count);
        Assert.All(report.Instances, instance => Assert.Equal(InstanceState.Available, instance.State));
        Assert.NotNull(report.Consistency);
        Assert.True(report.Consistency!.IsConsistent);
    }

    // --------------------------------------- Требование 4: обработка изменения и репликация

    [Fact]
    public async Task CreateBooking_IsWrittenToPrimaryAndReplicatedToReplica()
    {
        DbBookingService service = TestCluster.Create("replicate-create");

        Response created = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        ReservationReport report = TestDatabase.ReportOf(created)!;

        Assert.True(created.IsSuccess);
        Assert.Equal("Primary", report.ServedBy);
        Assert.Contains("REPLICATION Primary → Replica: OK", report.Replication);

        InstanceSnapshot replica = TestCluster.ReadOf(service, InstanceRole.Replica);
        Assert.Equal(1, replica.Bookings);
        Assert.Equal(1, replica.ActiveBookings);
        Assert.Equal(2, replica.AvailableRooms);

        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public async Task PayBooking_ReplicatesStatusChangeAndPaymentRow()
    {
        DbBookingService service = TestCluster.Create("replicate-pay");

        int bookingId = TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!.BookingId;

        Response paid = service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");
        var report = TestCluster.Data<ChangeReport>(paid);

        Assert.True(paid.IsSuccess);
        Assert.Equal("PayBookingCommitted", paid.EventTriggered);
        Assert.Equal("Primary", report.ServedBy);
        Assert.Contains("REPLICATION Primary → Replica: OK", report.Replication);

        InstanceSnapshot replica = TestCluster.ReadOf(service, InstanceRole.Replica);
        Assert.Equal(1, replica.Payments);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public async Task CancelBooking_ReplicatesFreedRoomToReplica()
    {
        DbBookingService service = TestCluster.Create("replicate-cancel");

        int bookingId = TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!.BookingId;

        Response cancelled = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.True(cancelled.IsSuccess);
        Assert.Equal("Primary", TestCluster.Data<ChangeReport>(cancelled).ServedBy);

        InstanceSnapshot replica = TestCluster.ReadOf(service, InstanceRole.Replica);
        Assert.Equal(0, replica.ActiveBookings);
        Assert.Equal(3, replica.AvailableRooms);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public async Task RejectedChange_TriggersNoReplicationAndKeepsCopiesEqual()
    {
        DbBookingService service = TestCluster.Create("replicate-rejected");

        await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        Response second = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 2), "Client 2");

        Assert.False(second.IsSuccess);
        Assert.Empty(TestDatabase.ReportOf(second)!.Replication);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public async Task ReplicationFailure_IsReportedButDoesNotCancelTheAcceptedChange()
    {
        DbBookingService service = TestCluster.Create("replication-error");
        service.Cluster!.Replica.Fail("replica is down for the replication error test");

        Response created = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");

        Assert.True(created.IsSuccess);
        Assert.Equal("ReplicationFailed", created.EventTriggered);
        Assert.Contains("REPLICATION FAILED", TestDatabase.ReportOf(created)!.Replication);

        Assert.Equal(1, TestCluster.PeekOf(service, InstanceRole.Primary).Bookings);
        Assert.Equal(0, TestCluster.PeekOf(service, InstanceRole.Replica).Bookings);
        Assert.False(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public async Task FullSynchronization_CopiesEveryRowAndKeepsSequencesUsable()
    {
        DbBookingService service = TestCluster.Create("full-sync");
        InstanceCluster cluster = service.Cluster!;

        int bookingId = TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!.BookingId;

        // Копия «отстаёт»: её содержимое портится напрямую, чтобы проверить полную синхронизацию.
        TestCluster.Execute($"DELETE FROM {TestCluster.ReplicaSchema}.bookings WHERE id = {bookingId}");
        TestCluster.Execute($"UPDATE {TestCluster.ReplicaSchema}.rooms SET is_available = TRUE WHERE id = 101");
        Assert.False(TestCluster.Compare(service).IsConsistent);

        ReplicationOutcome outcome = cluster.Replicator.Synchronize(cluster.Primary, cluster.Replica, "tester");

        Assert.True(outcome.IsSuccess);
        Assert.Equal(1, outcome.BookingRows);
        Assert.True(TestCluster.Compare(service).IsConsistent);

        // Новая бронь не должна повторить id, уже занятый в восстановленной копии.
        int next = TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(102, 2), "Client 1"))!.BookingId;

        Assert.NotEqual(bookingId, next);
    }

    [Fact]
    public async Task ReplicationDisabled_ReplicaStaysUntouched()
    {
        DbBookingService service = TestCluster.Create("replication-off", replication: false);

        Response created = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");

        Assert.Equal("Primary only (replication OFF)", service.Cluster!.Mode);
        Assert.Equal("Primary", TestDatabase.ReportOf(created)!.ServedBy);
        Assert.Empty(TestDatabase.ReportOf(created)!.Replication);
        Assert.Equal(1, TestCluster.ReadOf(service, InstanceRole.Primary).Bookings);
        Assert.Equal(0, TestCluster.ReadOf(service, InstanceRole.Replica).Bookings);
    }

    // ------------------------------------------------------- Требование 8: состояние сервиса

    [Fact]
    public async Task ResetDatabase_ClearsBothCopies()
    {
        DbBookingService service = TestCluster.Create("reset-both");

        await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");

        Assert.True(service.ResetDatabase("{}", "tester").IsSuccess);

        Assert.Equal(0, TestCluster.ReadOf(service, InstanceRole.Primary).Bookings);
        Assert.Equal(3, TestCluster.ReadOf(service, InstanceRole.Replica).AvailableRooms);
        Assert.True(TestCluster.Compare(service).IsConsistent);
    }

    [Fact]
    public void DatabaseHealth_ReportsClusterWithTwoCopies()
    {
        DbBookingService service = TestCluster.Create("health");

        Response response = service.DatabaseHealth("{}", "Client 1");

        Assert.True(response.IsSuccess);
        Assert.Contains("cluster is reachable", response.Message);
        Assert.Contains("replication ON", response.Message);
    }

    [Fact]
    public void SingleInstanceServer_RejectsEveryReplicationOperation()
    {
        DbBookingService service = TestDatabase.Create("no-cluster");

        var operations = new (string Name, Func<Response> Call)[]
        {
            ("InstanceStatus", () => service.InstanceStatus("{}", "Client 1")),
            ("FailInstance", () => service.FailInstance(TestCluster.RolePayload("primary"), "Client 1")),
            ("RestoreInstance", () => service.RestoreInstance(TestCluster.RolePayload("primary"), "Client 1")),
            ("CompareReplicas", () => service.CompareReplicas("{}", "Client 1")),
            ("SynchronizeReplicas",
                () => service.SynchronizeReplicas(TestCluster.SyncPayload("primary", "replica"), "Client 1"))
        };

        foreach ((string name, Func<Response> call) in operations)
        {
            Response response = call();

            Assert.False(response.IsSuccess);
            Assert.Equal($"{name} is available only when the server is started with --replication on",
                response.Message);
        }
    }

    [Fact]
    public void InvalidInstanceRole_IsRejectedWithoutChangingTheCluster()
    {
        DbBookingService service = TestCluster.Create("bad-role");

        Response unknown = service.FailInstance(TestCluster.RolePayload("standby"), "Client 1");
        Response malformed = service.FailInstance("{ not a json }", "Client 1");

        Assert.False(unknown.IsSuccess);
        Assert.Contains("unknown instance role 'standby'", unknown.Message);
        Assert.False(malformed.IsSuccess);
        Assert.Contains("instance role is missing", malformed.Message);

        Assert.All(service.Cluster!.Status(), status => Assert.Equal(InstanceState.Available, status.State));
    }
}
