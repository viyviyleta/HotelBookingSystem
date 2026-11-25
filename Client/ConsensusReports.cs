namespace Client;


/// Клиентские копии отчётов сервера о кластере узлов (ЛР №7). Клиент не ссылается на типы сервера:
/// он читает тот же JSON, который сервер положил в поле Data ответа (как в ЛР №4-№6).

public sealed record NodeReport(
    int Id,
    string Name,
    int Port,
    string Status,
    string Role,
    string Fault,
    string Reason,
    int MessagesSent,
    int MessagesHandled,
    int HeartbeatsAnswered,
    int VotesCast,
    int TimeoutsSeen,
    int KnownPeers,
    List<string> Peers,
    string LastLeaderSeen,
    string AgreedValue,
    string LastActivity);

public sealed record ClusterStatusReport(
    bool Enabled,
    string Summary,
    int Total,
    int Majority,
    int Available,
    string Leader,
    string ElectionReason,
    int Elections,
    int Proposals,
    int ConsensusAchieved,
    int ConsensusRefused,
    int HeartbeatsSent,
    int Timeouts,
    List<NodeReport> Nodes);

public sealed record NodeReply(
    int NodeId,
    string Node,
    string MessageType,
    string Result,
    string Status,
    string Role,
    int RoundTripMs,
    string Details,
    string Connection,
    int KnownPeers);

public sealed record JournalEntry(string Time, string Who, string Marker, string Text);

public sealed record HeartbeatReport(
    string Summary,
    string Initiator,
    List<NodeReply> Exchanges,
    List<string> FailedNodes,
    List<JournalEntry> Journal);

public sealed record ElectionReport(
    string Summary,
    string Reason,
    string Initiator,
    int PreviousLeader,
    int LeaderId,
    string LeaderName,
    int Majority,
    int Available,
    int Total,
    bool SingleLeader,
    int DurationMs,
    List<NodeReply> Polls,
    List<string> Excluded,
    List<JournalEntry> Journal);

public sealed record ConsensusReport(
    string Summary,
    string Proposal,
    string Description,
    string ProposedBy,
    int Majority,
    int Available,
    int Total,
    int Yes,
    int No,
    int NoResponse,
    int Refused,
    bool Achieved,
    bool Applied,
    string Decision,
    string AppliedNote,
    string EventName,
    int RoundMs,
    List<NodeReply> Votes,
    List<JournalEntry> Journal);

public sealed record ClusterJournalReport(int Returned, int Total, List<JournalEntry> Lines);

public sealed record ClusterStatisticsReport(
    string Summary,
    int Total,
    int Majority,
    int Elections,
    string CurrentLeader,
    int Proposals,
    int ConsensusAchieved,
    int ConsensusRefused,
    int HeartbeatsSent,
    int Timeouts,
    int MessagesSent,
    int MessagesAnswered,
    int VotesCast,
    List<NodeReport> Nodes,
    List<JournalEntry> Journal);
