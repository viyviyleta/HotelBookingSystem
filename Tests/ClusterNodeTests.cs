using Server.Consensus;
using Shared.Network;
using Xunit;

namespace Tests;

/// <summary>
/// Часть 1 ЛР №7 и Требование 3: узел с уникальным ID, состоянием, ролью, информацией о других узлах
/// и умением принимать решения по входящему сообщению. Сокетов здесь нет - проверяется сама логика узла.
/// </summary>
public sealed class ClusterNodeTests
{
    // --------------------------------------------------------------- Требование 4: создание узлов

    [Fact]
    public void ClusterNode_NewNode_HasOwnIdAvailableStateAndFollowerRole()
    {
        ClusterNode node = TestCluster7.Node(3);

        Assert.Equal(3, node.Id);
        Assert.Equal("Node 3", node.Name);
        Assert.Equal(NodeStatus.Available, node.Status);
        Assert.Equal(NodeRole.Follower, node.Role);
        Assert.True(node.IsAvailable);
        Assert.Equal("AVAILABLE", node.StateName());
        Assert.Equal("FOLLOWER", node.RoleName());
    }

    [Fact]
    public void ClusterNode_RememberPeer_KeepsInformationAboutOtherNodes()
    {
        ClusterNode node = TestCluster7.Node(1);

        node.RememberPeer(2, "AVAILABLE");
        node.RememberPeer(5, "FAILED");
        node.RememberPeer(1, "AVAILABLE");

        Assert.Equal(2, node.KnownPeers);
        Assert.Equal(new[] { "Node 2", "Node 5" }, node.Peers);
    }

    [Fact]
    public void ClusterNode_ApplyFault_State_ChangesStateToFailedAndLogsReason()
    {
        ClusterNode node = TestCluster7.Node(5);

        node.MarkFailed("node state changed to FAILED");

        Assert.Equal(NodeStatus.Failed, node.Status);
        Assert.False(node.IsAvailable);
        Assert.Equal(NodeFault.State, node.Fault);
        Assert.Equal(NodeRole.Follower, node.Role);
        Assert.Contains("FAILED", node.LastActivity);
    }

    [Fact]
    public void ClusterNode_Restore_ReturnsTheNodeToAvailableAndClearsTheFault()
    {
        ClusterNode node = TestCluster7.Node(2);
        node.ApplyFault(NodeFault.Stalled, "node stopped processing messages");

        node.Restore("node is back");

        Assert.Equal(NodeStatus.Available, node.Status);
        Assert.Equal(NodeFault.None, node.Fault);
        Assert.Equal("node is back", node.Reason);
    }

    // ------------------------------------------------------ Требование 3: обмен сообщениями узла

    [Fact]
    public void ClusterNode_HandleHeartbeat_AnswersWithItsOwnStateAndRole()
    {
        ClusterNode node = TestCluster7.Node(4);
        node.RememberPeer(1, "AVAILABLE");

        ClusterMessage? reply = node.Handle(TestCluster7.Message(MessageTypes.Heartbeat, from: 1, to: 4));

        Assert.NotNull(reply);
        Assert.Equal(MessageTypes.Alive, reply!.Type);
        Assert.Equal(4, reply.From);
        Assert.Equal(1, reply.To);
        Assert.Equal("AVAILABLE", reply.Status);
        Assert.Equal("FOLLOWER", reply.Role);
        Assert.Equal(1, reply.KnownPeers);
        Assert.Equal(1, node.HeartbeatsAnswered);
    }

    [Fact]
    public void ClusterNode_HandleHeartbeat_WhenTheNodeIsFailed_AnswersWithFailedState()
    {
        ClusterNode node = TestCluster7.Node(5);
        node.MarkFailed("node state changed to FAILED");

        ClusterMessage? reply = node.Handle(TestCluster7.Message(MessageTypes.Heartbeat, from: 1, to: 5));

        Assert.Equal("FAILED", reply!.Status);
        Assert.Contains("is FAILED", reply.Details);
    }

    [Fact]
    public void ClusterNode_HandleElection_RepliesThatItIsReadyToTakePart()
    {
        ClusterNode node = TestCluster7.Node(2);

        ClusterMessage? reply = node.Handle(TestCluster7.Message(MessageTypes.Election, from: 1, to: 2));

        Assert.Equal(MessageTypes.Election, reply!.Type);
        Assert.Contains("election", reply.Details);
        Assert.Equal(1, node.MessagesHandled);
    }

    [Fact]
    public void ClusterNode_HandleLeaderAnnounce_FollowerAcceptsTheOnlyLeader()
    {
        ClusterNode node = TestCluster7.Node(3);

        node.Handle(TestCluster7.Message(MessageTypes.Leader, from: 5, to: 3, value: "Node 5"));

        Assert.Equal(NodeRole.Follower, node.Role);
        Assert.Equal("Node 5", node.LastLeaderSeen);
    }

    [Fact]
    public void ClusterNode_HandleLeaderAnnounce_NodeThatHearsItselfBecomesLeader()
    {
        ClusterNode node = TestCluster7.Node(5);

        node.Handle(TestCluster7.Message(MessageTypes.Leader, from: 5, to: 5, value: "Node 5"));

        Assert.Equal(NodeRole.Leader, node.Role);
        Assert.True(node.IsLeader);
    }

    // ------------------------------------------------------------- Требование 4: голосование узла

    [Fact]
    public void ClusterNode_HandlePropose_AvailableNodeVotesYes()
    {
        ClusterNode node = TestCluster7.Node(1);

        ClusterMessage? vote = node.Handle(TestCluster7.Message(MessageTypes.Propose, from: 5, to: 1,
            subject: "BookingStatus", value: "Paid"));

        Assert.Equal(VoteResults.Yes, vote!.Vote);
        Assert.Equal("P1", vote.Proposal);
        Assert.Equal(1, node.VotesCast);
    }

    [Fact]
    public void ClusterNode_HandlePropose_NodeFromTheOpposeListVotesNo()
    {
        ClusterNode node = TestCluster7.Node(4);

        ClusterMessage? vote = node.Handle(TestCluster7.Message(MessageTypes.Propose, from: 5, to: 4,
            oppose: [4], subject: "BookingStatus", value: "Cancelled"));

        Assert.Equal(VoteResults.No, vote!.Vote);
        Assert.Contains("disagrees", vote.Details);
    }

    [Fact]
    public void ClusterNode_HandlePropose_FailedNodeRefusesToVote()
    {
        ClusterNode node = TestCluster7.Node(3);
        node.MarkFailed("the node declares itself FAILED");

        ClusterMessage? vote = node.Handle(TestCluster7.Message(MessageTypes.Propose, from: 5, to: 3,
            value: "Paid"));

        Assert.Equal(VoteResults.Refused, vote!.Vote);
        Assert.Contains("does not vote", vote.Details);
    }

    [Fact]
    public void ClusterNode_HandleCommit_RemembersTheAgreedValue()
    {
        ClusterNode node = TestCluster7.Node(2);

        node.Handle(TestCluster7.Message(MessageTypes.Commit, from: 5, to: 2,
            subject: "BookingStatus", value: "Paid"));

        Assert.Equal("BookingStatus=Paid", node.AgreedValue);
    }

    // ------------------------------------------------- Требование 4: четыре способа смоделировать отказ

    [Fact]
    public void ClusterNode_Handle_StoppedProcessing_DoesNotAnswerAnyMessage()
    {
        ClusterNode node = TestCluster7.Node(3);
        node.ApplyFault(NodeFault.Stalled, "node stopped processing messages");

        Assert.Null(node.Handle(TestCluster7.Message(MessageTypes.Heartbeat, from: 1, to: 3)));
        Assert.Null(node.Handle(TestCluster7.Message(MessageTypes.Propose, from: 1, to: 3)));
        Assert.Equal(2, node.MessagesDropped);
    }

    [Fact]
    public void ClusterNode_Handle_HeartbeatSilence_StillAnswersOtherMessages()
    {
        ClusterNode node = TestCluster7.Node(5);
        node.ApplyFault(NodeFault.Silent, "node stopped answering heartbeats");

        Assert.Null(node.Handle(TestCluster7.Message(MessageTypes.Heartbeat, from: 1, to: 5)));
        Assert.Null(node.Handle(TestCluster7.Message(MessageTypes.Election, from: 1, to: 5)));
        Assert.NotNull(node.Handle(TestCluster7.Message(MessageTypes.Status, from: 1, to: 5)));
    }

    [Fact]
    public void ClusterNode_Handle_ArtificialException_ThrowsInsteadOfAnswering()
    {
        ClusterNode node = TestCluster7.Node(2);
        node.ApplyFault(NodeFault.Exception, "artificial exception while handling a message");

        NodeProcessingException error = Assert.Throws<NodeProcessingException>(() =>
            node.Handle(TestCluster7.Message(MessageTypes.Heartbeat, from: 1, to: 2)));

        Assert.Contains("artificial exception", error.Message);
    }

    // -------------------------------------------------------------------- Формат сообщения в сети

    [Fact]
    public void ClusterMessage_ToRequest_CarriesTypeSenderAndBodyInOneJsonLine()
    {
        var message = TestCluster7.Message(MessageTypes.Propose, from: 5, to: 2, value: "Paid");

        Request request = message.ToRequest();

        Assert.Equal(MessageTypes.Propose, request.Operation);
        Assert.Equal("Node 5", request.ClientName);
        Assert.Contains("\"to\":2", request.Payload);
    }

    [Fact]
    public void ClusterMessage_FromRequest_ReadsTheSenderEvenWhenTheBodyHasNoId()
    {
        var request = new Request
        {
            Operation = MessageTypes.Heartbeat,
            ClientName = "Node 4",
            Payload = "{\"type\":\"HEARTBEAT\",\"to\":1}"
        };

        ClusterMessage? message = ClusterMessage.FromRequest(request);

        Assert.Equal(4, message!.From);
        Assert.Equal(1, message.To);
        Assert.Equal(MessageTypes.Heartbeat, message.Type);
    }

    [Fact]
    public void ClusterMessage_FromRequest_BrokenJsonIsRefusedInsteadOfThrowing()
    {
        var request = new Request { Operation = "HEARTBEAT", Payload = "{ not json" };

        Assert.Null(ClusterMessage.FromRequest(request));
    }

    [Theory]
    [InlineData("HEARTBEAT", true)]
    [InlineData("PROPOSE", true)]
    [InlineData("ALIVE", false)]
    [InlineData("VOTE", false)]
    [InlineData("PING", false)]
    public void MessageTypes_IsRequest_AcceptsOnlyRequests(string type, bool expected) =>
        Assert.Equal(expected, MessageTypes.IsRequest(type));
}
