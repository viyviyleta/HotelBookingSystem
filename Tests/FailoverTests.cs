using System.Text.Json;
using Server;
using Server.Database;
using Server.Replication;
using Shared.Models;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 3 ЛР №5: определение отказа Primary и обслуживание запросов доступной Replica.
/// </summary>
public sealed class FailoverTests : IDisposable
{
    public void Dispose()
    {
        TestCluster.Cleanup();
        TestDatabase.Cleanup();
    }

    private static Response FailInstance(DbBookingService service, string role) =>
        service.FailInstance(TestCluster.RolePayload(role), "Controller");

    // ------------------------------------------- Требование 4: определение недоступного Primary

    [Fact]
    public void FailInstance_MarksPrimaryUnavailableAndKeepsReplicaAlive()
    {
        DbBookingService service = TestCluster.Create("fail-primary");

        Response response = FailInstance(service, "primary");
        var status = TestCluster.Data<InstanceStatus>(response);

        Assert.True(response.IsSuccess);
        Assert.Equal(InstanceState.Unavailable, status.State);
        Assert.Equal(TestCluster.PrimarySchema, status.Schema);
        Assert.False(service.Cluster!.Primary.IsAvailable);
        Assert.True(service.Cluster!.Replica.IsAvailable);
    }

    [Fact]
    public void InstanceStatus_WhenPrimaryIsDown_ReportsReplicaAsServing()
    {
        DbBookingService service = TestCluster.Create("status-during-outage");
        FailInstance(service, "primary");

        Response response = service.InstanceStatus("{}", "Client 1");
        var report = TestCluster.Data<ClusterReport>(response);

        Assert.True(response.IsSuccess);
        Assert.Equal("Replica", report.Serving);
        Assert.Equal(InstanceState.Unavailable,
            report.Instances.Single(instance => instance.Role == InstanceRole.Primary).State);
    }

    // ------------------------------------------- Требование 4: выбор доступной Replica

    [Fact]
    public void Route_WhenPrimaryIsUnavailable_SelectsReplica()
    {
        DbBookingService service = TestCluster.Create("route");
        InstanceCluster cluster = service.Cluster!;
        cluster.Primary.Fail("primary is down for the routing test");

        DatabaseInstance served = cluster.Route("Client 1");

        Assert.Same(cluster.Replica, served);
        Assert.Equal(InstanceRole.Replica, served.Role);
    }

    [Fact]
    public void Route_WhenBothInstancesAreUnavailable_ReportsFailoverAsImpossible()
    {
        DbBookingService service = TestCluster.Create("route-both-down");
        InstanceCluster cluster = service.Cluster!;
        cluster.Primary.Fail("primary is down");
        cluster.Replica.Fail("replica is down too");

        InstanceUnavailableException error =
            Assert.Throws<InstanceUnavailableException>(() => cluster.Route("Client 1"));

        Assert.Contains("both data instances are UNAVAILABLE", error.Message);
        Assert.Contains("replica is down too", error.Message);
    }

    // ---------------------------------- Требование 4: успешное выполнение запроса после Failover

    [Fact]
    public async Task CheckAvailability_DuringPrimaryOutage_IsServedByReplica()
    {
        DbBookingService service = TestCluster.Create("read-during-outage");
        FailInstance(service, "primary");

        Response read = await service.CheckAvailabilityAsync("{}", "Client 1");

        Assert.True(read.IsSuccess);
        Assert.Contains("Replica", read.Message);
        Assert.Equal(3, JsonSerializer.Deserialize<List<Room>>(read.Data)!.Count);
    }

    [Fact]
    public async Task CreateBooking_DuringPrimaryOutage_IsWrittenToReplicaOnly()
    {
        DbBookingService service = TestCluster.Create("write-during-outage");
        FailInstance(service, "primary");

        Response created = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        ReservationReport report = TestDatabase.ReportOf(created)!;

        Assert.True(created.IsSuccess);
        Assert.Equal("Replica", report.ServedBy);
        Assert.Equal("ReplicationFailed", created.EventTriggered);
        Assert.Equal(1, TestCluster.PeekOf(service, InstanceRole.Replica).Bookings);
        Assert.Equal(0, TestCluster.PeekOf(service, InstanceRole.Primary).Bookings);
    }

    [Fact]
    public async Task OccupiedRoom_DuringOutage_IsStillRejectedForTheSecondClient()
    {
        DbBookingService service = TestCluster.Create("two-clients-outage");
        FailInstance(service, "primary");
        await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");

        Response second = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 2), "Client 2");

        Assert.False(second.IsSuccess);
        Assert.Contains("already reserved", second.Message);
        Assert.Equal("Replica", TestDatabase.ReportOf(second)!.ServedBy);
    }

    // -------------------------------------------------- Требование 8: отказ обоих экземпляров

    [Fact]
    public async Task Requests_AreRefusedWhenBothInstancesAreDown()
    {
        DbBookingService service = TestCluster.Create("total-outage");
        FailInstance(service, "primary");
        FailInstance(service, "replica");

        Response read = await service.CheckAvailabilityAsync("{}", "Client 1");
        Response reserve = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");

        Assert.False(read.IsSuccess);
        Assert.Contains("both data instances are UNAVAILABLE", read.Message);

        Assert.False(reserve.IsSuccess);
        Assert.Equal("FailoverImpossible", reserve.EventTriggered);
        Assert.Equal(0, TestCluster.PeekOf(service, InstanceRole.Replica).Bookings);
    }

    [Fact]
    public void DatabaseHealth_WhenBothInstancesAreDown_ReportsThemBoth()
    {
        DbBookingService service = TestCluster.Create("health-total-outage");
        FailInstance(service, "primary");
        FailInstance(service, "replica");

        Response health = service.DatabaseHealth("{}", "Client 1");

        Assert.False(health.IsSuccess);
        Assert.Contains("Both instances are unavailable", health.Message);
    }

    // ------------------------------------ Реальная недоступность копии: настоящая ошибка соединения

    [Fact]
    public void HealthProbe_WhenReplicaDatabaseIsMissing_MarksReplicaUnavailable()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("real-probe");

        InstanceStatus status = service.Cluster!.Replica.Inspect();

        Assert.Equal(InstanceState.Unavailable, status.State);
        Assert.Contains("riot-lr5-missing", status.Target);
        Assert.StartsWith("connection error detected by health check", status.Reason);
    }

    [Fact]
    public async Task Requests_AreStillServedByPrimary_WhenReplicaDatabaseIsMissing()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("dead-replica");
        service.Cluster!.Replica.Inspect();

        Response read = await service.CheckAvailabilityAsync("{}", "Client 1");

        Assert.True(read.IsSuccess);
        Assert.Contains("Primary", read.Message);
    }

    [Fact]
    public async Task Replication_ToMissingPhysicalCopy_FailsWithRealConnectionError()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("replica-gone");
        service.Cluster!.Replica.Inspect();

        Response created = await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1");
        ReservationReport report = TestDatabase.ReportOf(created)!;

        Assert.True(created.IsSuccess);
        Assert.Equal("Primary", report.ServedBy);
        Assert.Equal("ReplicationFailed", created.EventTriggered);
        Assert.Contains("does not exist", report.Replication, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CompareReplicas_WhenOneCopyIsPhysicallyGone_ReportsComparisonFailure()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("compare-gone");

        Response response = service.CompareReplicas("{}", "Client 1");

        Assert.False(response.IsSuccess);
        Assert.Equal("ComparisonFailed", response.EventTriggered);
        Assert.Contains("comparison is impossible", response.Message);
    }

    [Fact]
    public void InstanceStatus_WhenOneCopyIsPhysicallyGone_StillNamesTheServingCopy()
    {
        DbBookingService service = TestCluster.CreateWithUnreachableReplica("status-gone");

        Response response = service.InstanceStatus("{}", "Client 1");
        var report = TestCluster.Data<ClusterReport>(response);

        Assert.True(response.IsSuccess);
        Assert.Equal("Primary", report.Serving);
        Assert.Null(report.Consistency);
        Assert.Contains("comparison is impossible", response.Message);
    }

    [Fact]
    public void ReadThroughUnavailableInstance_IsRefusedButItsDataSurvives()
    {
        DbBookingService service = TestCluster.Create("refused-read");
        InstanceCluster cluster = service.Cluster!;
        cluster.Primary.Fail("primary is down");

        Assert.Throws<InstanceUnavailableException>(() => cluster.Replicator.ReadCopy(cluster.Primary, "tester"));

        Assert.Equal(3, TestCluster.PeekOf(service, InstanceRole.Primary).Rooms);
    }
}
