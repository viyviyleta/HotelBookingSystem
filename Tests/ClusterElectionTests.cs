using Server.Consensus;
using Xunit;

namespace Tests;

/// <summary>
/// Части 2-4 ЛР №7: выбор лидера, heartbeat-проверка доступности, обнаружение отказа и повторный выбор.
/// Здесь узлы уже работают на реальных TCP-портах, поэтому отказ соседа видно по таймауту или по
/// отказу в подключении, а не по прямой записи в состояние.
/// </summary>
public sealed class ClusterElectionTests
{
    // ------------------------------------------------------------- Требование 4: узлы и выборы

    [Fact]
    public void Cluster_Start_GivesEveryNodeItsOwnIdAndFreePort()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        Assert.Equal(5, cluster.Total);
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, cluster.Nodes.Select(node => node.Id));
        Assert.Equal(5, cluster.Nodes.Select(node => node.Port).Distinct().Count());
        Assert.All(cluster.Nodes, node => Assert.True(node.Port > 0));
        Assert.Equal(5, cluster.AvailableCount);
    }

    [Fact]
    public void Cluster_Start_ElectsTheAvailableNodeWithTheHighestId()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        Assert.Equal(5, cluster.LeaderId);
        Assert.Equal(NodeRole.Leader, cluster.Nodes[4].Role);
        Assert.All(cluster.Nodes.Take(4), node => Assert.Equal(NodeRole.Follower, node.Role));
    }

    [Fact]
    public void Elect_RollsOutAvailabilityOverTcp_AndReportsEveryNode()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ElectionReport report = cluster.Elect("check the topology", "Tester");

        Assert.Equal(5, report.LeaderId);
        Assert.Equal(5, report.Available);
        Assert.Equal(5, report.Polls.Count);
        Assert.Empty(report.Excluded);
        Assert.True(report.SingleLeader);
    }

    [Fact]
    public void Elect_ExcludesTheNodeThatStoppedAnswering()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(5, "silent", "the node stopped answering heartbeats", "Tester");

        ElectionReport report = cluster.Elect("Node 5 is silent", "Tester");

        Assert.Equal(4, report.LeaderId);
        Assert.Equal(["Node 5"], report.Excluded);
        Assert.Equal(NodeStatus.Failed, cluster.Nodes[4].Status);
    }

    [Fact]
    public void Elect_KeepsExactlyOneLeader_WhenNodesFailOneAfterAnother()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.FailNode(5, "offline", "node 5 is down", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("node 5 is down", "Tester");

        cluster.FailNode(4, "stalled", "node 4 stopped processing", "Tester");
        cluster.Probe("Tester");
        ElectionReport last = cluster.Elect("node 4 is stalled", "Tester");

        Assert.Equal(3, last.LeaderId);
        Assert.Equal(1, cluster.Nodes.Count(node => node.Role == NodeRole.Leader && node.IsAvailable));
        Assert.True(last.SingleLeader);
    }

    [Fact]
    public void Elect_WhenNoNodeIsAvailable_LeavesTheClusterWithoutLeader()
    {
        using ClusterManager cluster = TestCluster7.Cluster(3);

        foreach (ClusterNode node in cluster.Nodes)
            cluster.FailNode(node.Id, "state", "every node is down", "Tester");

        ElectionReport report = cluster.Elect("all nodes are down", "Tester");

        Assert.Equal(0, report.LeaderId);
        Assert.Equal("none", report.LeaderName);
        Assert.False(report.SingleLeader);
    }

    // ----------------------------------------------------- Требование 4: отказ лидера и heartbeat

    [Fact]
    public void Probe_WhenEveryNodeIsAlive_AnswersAlive()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        HeartbeatReport report = cluster.Probe("Tester");

        Assert.Equal(5, report.Exchanges.Count);
        Assert.Empty(report.FailedNodes);
        Assert.Equal(0, cluster.Nodes.Count(node => node.Status == NodeStatus.Failed));
    }

    [Fact]
    public void Probe_WhenTheLeaderStopsAnswering_MarksItFailedAndClearsTheLeader()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(5, "silent", "the leader stopped answering heartbeats", "Tester");

        HeartbeatReport report = cluster.Probe("Tester");

        Assert.Equal(["Node 5"], report.FailedNodes);
        Assert.Equal(0, cluster.LeaderId);
        Assert.Equal(NodeStatus.Failed, cluster.Nodes[4].Status);
        Assert.Equal(NodeRole.Follower, cluster.Nodes[4].Role);
    }

    [Fact]
    public void Probe_WhenANodeIsOffline_ReportsItAsUnreachable()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(2, "offline", "node process is down", "Tester");

        HeartbeatReport report = cluster.Probe("Tester");

        NodeReply aboutNode2 = report.Exchanges.Single(reply => reply.NodeId == 2);
        Assert.Equal("UNREACHABLE", aboutNode2.Result);
        Assert.Equal(NodeStatus.Failed, cluster.Nodes[1].Status);
    }

    [Theory]
    [InlineData("state")]
    [InlineData("stalled")]
    [InlineData("exception")]
    [InlineData("silent")]
    [InlineData("offline")]
    public void Failure_EveryWayFromTheHandout_IsDetectedAndRecorded(string mode)
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.FailNode(3, mode, $"failure detected by {mode}", "Tester");
        HeartbeatReport report = cluster.Probe("Tester");

        Assert.Contains("Node 3", report.FailedNodes);
        Assert.Equal(NodeStatus.Failed, cluster.Nodes[2].Status);
        Assert.Equal(4, cluster.AvailableCount);
        Assert.Contains(cluster.RecentJournal(60),
            entry => entry.Marker == "FAILURE" && entry.Text.Contains("Node 3"));
    }

    [Fact]
    public void ReElection_AfterTheLeaderFails_ElectsTheSecondHighestId()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(5, "silent", "leader is silent", "Tester");
        cluster.Probe("Tester");

        ElectionReport report = cluster.Elect("re-election after the leader failed", "Tester");

        Assert.Equal(4, report.LeaderId);
        Assert.Equal(5, report.PreviousLeader);
        Assert.Equal(NodeRole.Leader, cluster.Nodes[3].Role);
    }

    [Fact]
    public void RestoreNode_ReturnsTheNodeToTheCluster_AndAnElectionGivesItTheLeadAgain()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(5, "offline", "node 5 is down", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("node 5 is down", "Tester");

        Assert.Equal(4, cluster.LeaderId);

        cluster.RestoreNode(5, "node 5 is back", "Tester");
        ElectionReport report = cluster.Elect("node 5 is back", "Tester");

        Assert.Equal(5, report.LeaderId);
        Assert.Equal(NodeStatus.Available, cluster.Nodes[4].Status);
    }

    [Fact]
    public void RestoreNode_FailedNodeJoinsAsFollower_SoTheCurrentLeaderIsNotDoubled()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(3, "offline", "node 3 is down", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("node 3 is down", "Tester");

        cluster.RestoreNode(3, "node 3 is back", "Tester");

        Assert.Equal(NodeRole.Follower, cluster.Nodes[2].Role);
        Assert.Equal(1, cluster.Nodes.Count(node => node.Role == NodeRole.Leader && node.IsAvailable));
    }

    // -------------------------------------------------------------------- Сообщения между узлами

    [Fact]
    public void Cluster_AnswersSeveralMessagesAtTheSameTime()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        ClusterNode from = cluster.Nodes[0];
        ClusterNode to = cluster.Nodes[2];

        int handledBefore = to.MessagesHandled;

        List<NodeReply> replies = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() => NodeChannel.Request(from, to.Id, to.Port,
                TestCluster7.Message(MessageTypes.Heartbeat, from: from.Id, to: to.Id), cluster.TimeoutMs)))
            .Select(task => task.Result)
            .ToList();

        Assert.Equal(4, replies.Count(reply => reply.Result == MessageTypes.Alive));
        Assert.Equal(4, to.MessagesHandled - handledBefore);
    }

    [Fact]
    public void SendMessage_UnknownNode_IsReportedAsAnError()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ClusterResult result = cluster.SendMessage(99, 1, MessageTypes.Heartbeat, "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("Unknown node in the message", result.Message);
    }

    [Fact]
    public void SendMessage_FromANodeToItself_IsRefused()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ClusterResult result = cluster.SendMessage(2, 2, MessageTypes.Status, "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("cannot message itself", result.Message);
    }

    [Fact]
    public void Broadcast_ReplyType_IsRefusedBecauseOnlyRequestsCanBeSent()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ClusterResult result = cluster.Broadcast(MessageTypes.Alive, "Tester");

        Assert.False(result.IsSuccess);
        Assert.Contains("only request types", result.Message);
    }

    [Fact]
    public void Broadcast_Status_ReachesEveryLiveNodeInParallel()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);
        cluster.FailNode(4, "offline", "node 4 is down", "Tester");

        ClusterResult result = cluster.Broadcast(MessageTypes.Status, "Tester");

        Assert.True(result.IsSuccess);
        Assert.Contains("Node 4 = UNREACHABLE", result.Message);
        Assert.Contains("Node 1 = AVAILABLE", result.Message);
    }

    // ------------------------------------------------------------------ Журнал и счётчики

    [Fact]
    public void Journal_RecordsElectionRolesFailureVotingAndDecision()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.Propose(new Proposal("BookingStatus", "Paid", 7), "Tester");
        cluster.FailNode(5, "silent", "leader is silent", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("leader is silent", "Tester");
        cluster.Propose(new Proposal("BookingStatus", "Cancelled", 7), "Tester");

        IReadOnlyList<JournalEntry> lines = cluster.RecentJournal(200);

        Assert.Contains(lines, entry => entry.Marker == "ELECTION" && entry.Text.Contains("Election started"));
        Assert.Contains(lines, entry => entry.Marker == "ELECTION" && entry.Text.Contains("elected as LEADER"));
        Assert.Contains(lines, entry => entry.Marker == "FAILURE" && entry.Text.Contains("HEARTBEAT TIMEOUT"));
        Assert.Contains(lines, entry => entry.Marker == "ROLE");
        Assert.Contains(lines, entry => entry.Marker == "CONSENSUS" && entry.Text.Contains("CONSENSUS ACHIEVED"));
        Assert.Contains(lines, entry => entry.Marker == "VOTE" && entry.Text.Contains("YES ="));
    }

    [Fact]
    public void Status_ListsEveryNodeWithRoleStateAndPort()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        ClusterStatusReport report = cluster.Status();

        Assert.Equal(5, report.Total);
        Assert.Equal(3, report.Majority);
        Assert.Equal("Node 5", report.Leader);
        Assert.Equal(5, report.Nodes.Count);
        Assert.All(report.Nodes, node => Assert.True(node.Port > 0));
        Assert.All(report.Nodes, node => Assert.Equal($"Node {node.Id}", node.Name));
    }

    [Fact]
    public void Statistics_CountsElectionsProposalsAndTimeouts()
    {
        using ClusterManager cluster = TestCluster7.Cluster(5);

        cluster.FailNode(5, "silent", "leader is silent", "Tester");
        cluster.Probe("Tester");
        cluster.Elect("leader is silent", "Tester");
        cluster.Propose(new Proposal("ConfigurationVersion", "12"), "Tester");

        ClusterStatisticsReport report = cluster.Statistics(5);

        Assert.Equal(2, report.Elections);
        Assert.Equal(1, report.Proposals);
        Assert.Equal(1, report.ConsensusAchieved);
        Assert.True(report.Timeouts > 0);
        Assert.True(report.HeartbeatsSent > 0);
        Assert.Equal("Node 4", report.CurrentLeader);
    }
}
