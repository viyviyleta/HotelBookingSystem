using System.Text.Json;
using Server;
using Server.Consensus;
using Server.Messaging;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 5-6 ЛР №7 и Требование 4: большинство, голосование, достижение и недостижение консенсуса,
/// применение согласованного состояния предметной области.
/// </summary>
public sealed class ClusterConsensusTests
{
    // ------------------------------------------------------------------- Требование 4: большинство

    [Theory]
    [InlineData(5, 3)]
    [InlineData(4, 3)]
    [InlineData(3, 2)]
    [InlineData(2, 2)]
    [InlineData(1, 1)]
    public void MajorityOf_IsMoreThanHalfOfTheWholeCluster(int total, int expected) =>
        Assert.Equal(expected, ClusterManager.MajorityOf(total));

    [Fact]
    public void Majority_CountsTheWholeClusterNotTheAvailableNodes()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        foreach (int node in new[] { 3, 4, 5 })
        {
            cluster.FailNode(node, "offline", $"node {node} is down", "Tester");
            cluster.Probe("Tester");
        }

        Assert.Equal(2, cluster.AvailableCount);
        Assert.Equal(3, cluster.Majority);
    }

    // ------------------------------------------------------------------ Требование 4: голосование

    [Fact]
    public void Propose_EveryAvailableNodeVotesYes()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");

        Assert.True(report.Achieved);
        Assert.Equal(5, report.Yes);
        Assert.Equal(0, report.No);
        Assert.Equal(0, report.NoResponse);
        Assert.Equal(0, report.Refused);
        Assert.Equal(3, report.Majority);
        Assert.Equal("ACHIEVED", report.Decision);
        Assert.Equal("Node 5", report.ProposedBy);
    }

    [Fact]
    public void Propose_EveryNodeIsAskedOnce_AndTheLeaderVotesToo()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");

        Assert.Equal(5, report.Votes.Count);
        Assert.Equal([1, 2, 3, 4, 5], report.Votes.Select(vote => vote.NodeId).OrderBy(id => id));
        Assert.Contains(report.Votes, vote => vote.Node == "Node 5" && vote.Connection.Contains("itself"));
    }

    [Fact]
    public void Propose_NodesFromTheOpposeListVoteNo()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ConsensusReport report = cluster.Propose(
            new Proposal("BookingStatus", "Cancelled", 7, [1, 2]), "Tester");

        Dictionary<string, string> votes = TestCluster7.VoteByNode(report);

        Assert.Equal(VoteResults.No, votes["Node 1"]);
        Assert.Equal(VoteResults.No, votes["Node 2"]);
        Assert.Equal(VoteResults.Yes, votes["Node 5"]);
        Assert.Equal(3, report.Yes);
        Assert.Equal(2, report.No);
        Assert.True(report.Achieved);
    }

    [Fact]
    public void Propose_WithMajority_AppliesTheAgreedValueOnce()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        var applied = new List<string>();

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester",
            proposal =>
            {
                applied.Add(proposal.Description);
                return new AppliedDecision(true, "booking 7 status → Paid", "BookingCancelled");
            });

        Assert.True(report.Achieved);
        Assert.True(report.Applied);
        Assert.Equal(["BookingStatus=Paid (booking 7)"], applied);
        Assert.Contains("status → Paid", report.AppliedNote);
        Assert.Equal("BookingCancelled", report.EventName);
    }

    [Fact]
    public void Propose_WithoutMajority_AchievesNothingAndAppliesNothing()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        bool called = false;

        foreach (int node in new[] { 3, 4, 5 })
        {
            cluster.FailNode(node, "offline", "experiment 4", "Tester");
            cluster.Probe("Tester");
        }

        cluster.Elect("only two nodes are left", "Tester");

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester",
            _ =>
            {
                called = true;
                return new AppliedDecision(true, "should not happen");
            });

        Assert.Equal(2, report.Available);
        Assert.Equal(2, report.Yes);
        Assert.False(report.Achieved);
        Assert.False(report.Applied);
        Assert.False(called);
        Assert.Equal("NOT ACHIEVED", report.Decision);
        Assert.Contains("CONSENSUS NOT ACHIEVED", report.Summary);
    }

    [Fact]
    public void Propose_SilentNodeCountsAsNoResponse_ButMajorityStillWorks()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(3, "stalled", "node 3 stopped processing", "Tester");

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");

        Assert.Equal(1, report.NoResponse);
        Assert.Equal(4, report.Yes);
        Assert.True(report.Achieved);
        Assert.Equal(VoteResults.NoResponse, TestCluster7.VoteByNode(report)["Node 3"]);
    }

    [Fact]
    public void Propose_FailedNodeRefusesToVote()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(3, "state", "the node declares itself FAILED", "Tester");

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");

        Assert.Equal(1, report.Refused);
        Assert.Equal(4, report.Yes);
        Assert.True(report.Achieved);
        Assert.Contains("does not vote",
            report.Votes.Single(vote => vote.NodeId == 3).Details);
    }

    [Fact]
    public void Propose_WithoutALeader_IsRefusedAndNothingIsApplied()
    {
        using ClusterManager cluster = TestCluster7.Cluster(3);

        foreach (ClusterNode node in cluster.Nodes)
            cluster.FailNode(node.Id, "state", "every node is down", "Tester");

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester",
            _ => new AppliedDecision(true, "must not run"));

        Assert.False(report.Achieved);
        Assert.False(report.Applied);
        Assert.Empty(report.Votes);
        Assert.Contains("no available leader", report.Summary);
    }

    [Fact]
    public void Propose_AfterReElection_TheNewLeaderCoordinatesTheVote()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(5, "silent", "the leader stopped answering heartbeats", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("leader is silent", "Tester");

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");

        Assert.Equal("Node 4", report.ProposedBy);
        Assert.True(report.Achieved);
        Assert.Equal(4, report.Yes);
    }

    [Fact]
    public void Propose_AchievedValueIsCommittedToTheOtherNodes()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester",
            _ => new AppliedDecision(true, "booking 7 → Paid"));

        Assert.Contains(cluster.Nodes.Where(node => node.Id != 5),
            node => node.AgreedValue == "BookingStatus=Paid");
    }

    [Fact]
    public void Propose_WhenApplyingThrows_ConsensusIsRecordedButTheValueIsNotApplied()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ConsensusReport report = cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester",
            _ => throw new InvalidOperationException("the database refused the update"));

        Assert.True(report.Achieved);
        Assert.False(report.Applied);
        Assert.Contains("applying failed", report.AppliedNote);
    }

    [Fact]
    public void Propose_TwoRoundsInParallel_AreSerializedAndBothComplete()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        Task<ConsensusReport> first = Task.Run(() =>
            cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Client 1"));
        Task<ConsensusReport> second = Task.Run(() =>
            cluster.Propose(new Proposal("BookingStatus", "Cancelled", 8), "Client 2"));

        Task.WhenAll(first, second).GetAwaiter().GetResult();

        Assert.True(first.Result.Achieved);
        Assert.True(second.Result.Achieved);
        Assert.Equal(2, cluster.Status().Proposals);
        Assert.Equal(2, cluster.Status().ConsensusAchieved);
    }

    [Fact]
    public void VoteJournal_RecordsEveryAnswerCountAndDecision()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.Propose(new Proposal("BookingStatus", "Paid", 7, [1]), "Tester");
        IReadOnlyList<JournalEntry> lines = cluster.RecentJournal(60);

        Assert.Contains(lines, entry => entry.Marker == "CONSENSUS" && entry.Text.Contains("PROPOSE BookingStatus=Paid"));
        Assert.Contains(lines, entry => entry.Marker == "VOTE" && entry.Text.Contains("→ NO"));
        Assert.Contains(lines, entry => entry.Marker == "VOTE" && entry.Text.Contains("YES = 4"));
        Assert.Contains(lines, entry => entry.Marker == "CONSENSUS" && entry.Text.Contains("Majority = 3"));
        Assert.Contains(lines, entry => entry.Marker == "CONSENSUS" && entry.Text.Contains("CONSENSUS ACHIEVED"));
    }

    // ---------------------------------------- Требование 3: клиент и запись состояния по консенсусу

    [Fact]
    public async Task DbBookingService_CancelBooking_AppliesStatusOnlyAfterMajority()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        DbBookingService service = TestDatabase.Create("lr7-cancel", consensus: cluster);
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        Response cancelled = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.True(cancelled.IsSuccess);
        Assert.Contains("ACHIEVED", cancelled.EventTriggered);
        Assert.Equal("Cancelled", StatusOf(service, bookingId));
    }

    [Fact]
    public async Task DbBookingService_CancelBooking_WithoutMajority_WritesNothing()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        DbBookingService service = TestDatabase.Create("lr7-no-majority", consensus: cluster);
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        foreach (int node in new[] { 3, 4, 5 })
        {
            cluster.FailNode(node, "offline", "no majority experiment", "Tester");
            cluster.Probe("Tester");
        }

        cluster.Elect("only two nodes are left", "Tester");

        Response cancelled = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.False(cancelled.IsSuccess);
        Assert.Equal("ConsensusNotAchieved", cancelled.EventTriggered);
        Assert.Equal("Created", StatusOf(service, bookingId));
    }

    [Fact]
    public async Task DbBookingService_PayBooking_ThroughConsensus_PublishesTheDomainEvent()
    {
        TestPubSub.FastDelays();
        using ClusterManager cluster = TestCluster7.Cluster(5);
        MessagingHub hub = MessagingHub.CreateDefault("Tester");
        DbBookingService service = TestDatabase.Create("lr7-pay-event", hub: hub, consensus: cluster);
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        Response paid = service.PayBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");
        hub.WaitForDeliveries("Tester");

        Assert.True(paid.IsSuccess);
        Assert.Equal("Paid", StatusOf(service, bookingId));
        DomainEvent published = hub.PublishedEvents.Last();
        Assert.Equal(EventType.PaymentCompleted, published.EventType);
        Assert.True(hub.Received("NotificationService", published.EventId));
    }

    [Fact]
    public async Task DbBookingService_WithoutCluster_WritesDirectlyAsInPreviousLabs()
    {
        DbBookingService service = TestDatabase.Create("lr7-no-cluster");
        int bookingId = (TestDatabase.ReportOf(
            await service.CreateBookingAsync(TestDatabase.BookingPayload(101, 1), "Client 1"))!).BookingId;

        Response cancelled = service.CancelBooking(TestDatabase.PayloadWithBookingId(bookingId), "Client 1");

        Assert.True(cancelled.IsSuccess);
        Assert.DoesNotContain("CONSENSUS", cancelled.Message);
        Assert.Equal("Cancelled", StatusOf(service, bookingId));
    }

    [Fact]
    public void DbBookingService_ClusterOperations_WithoutClusterAnswerWithExplanation()
    {
        DbBookingService service = TestDatabase.Create("lr7-cluster-off");

        Response status = service.ClusterStatus("{}", "Client 1");

        Assert.False(status.IsSuccess);
        Assert.Contains("--cluster on", status.Message);
    }

    private static string StatusOf(DbBookingService service, int bookingId)
    {
        var snapshot = JsonSerializer.Deserialize<DatabaseSnapshotReport>(
            service.DatabaseSnapshot("{}", "Client 1").Data,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;

        return snapshot.Bookings.Single(booking => booking.Id == bookingId).Status;
    }
}
