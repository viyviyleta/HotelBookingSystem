using System.Collections.Concurrent;

namespace Server.Consensus;

public sealed class ClusterNode(int id, ClusterJournal journal)
{
    private readonly ConcurrentDictionary<int, string> _peers = new();

    private volatile NodeStatus _status = NodeStatus.Available;
    private volatile NodeRole _role = NodeRole.Follower;
    private volatile NodeFault _fault = NodeFault.None;
    private volatile string _reason = "running";
    private volatile string _lastLeaderSeen = "none";
    private volatile string _agreedValue = "none";
    private volatile string _lastActivity = "started";
    private int _port;

    private int _sent;
    private int _handled;
    private int _heartbeatsAnswered;
    private int _votesCast;
    private int _timeoutsSeen;
    private int _dropped;

    public int Id { get; } = id;

    public string Name => $"Node {Id}";

    /// <summary>Порт, на котором узел принимает сообщения соседей. После привязки слушателя становится известен.</summary>
    public int Port => Volatile.Read(ref _port);

    public NodeStatus Status => _status;

    public NodeRole Role => _role;

    public NodeFault Fault => _fault;

    public string Reason => _reason;

    public string LastLeaderSeen => _lastLeaderSeen;

    public string AgreedValue => _agreedValue;

    public string LastActivity => _lastActivity;

    public bool IsAvailable => _status == NodeStatus.Available;

    public bool IsLeader => _role == NodeRole.Leader && IsAvailable;

    public int MessagesSent => Volatile.Read(ref _sent);

    public int MessagesHandled => Volatile.Read(ref _handled);

    public int HeartbeatsAnswered => Volatile.Read(ref _heartbeatsAnswered);

    public int VotesCast => Volatile.Read(ref _votesCast);

    public int TimeoutsSeen => Volatile.Read(ref _timeoutsSeen);

    public int MessagesDropped => Volatile.Read(ref _dropped);

    public IReadOnlyList<string> Peers => _peers.Keys.OrderBy(peer => peer).Select(peer => $"Node {peer}").ToList();

    public int KnownPeers => _peers.Count;

    /// <summary>Узел запоминает соседа и его последнюю роль - это «информация о других узлах» из Части 1.</summary>
    public void RememberPeer(int id, string state)
    {
        if (id == Id || id <= 0)
            return;

        _peers[id] = state;
    }

    public void ForgetPeer(int id) => _peers.TryRemove(id, out _);

    public void Bind(int port) => Volatile.Write(ref _port, port);

    public void CountSent() => Interlocked.Increment(ref _sent);

    public void CountTimeout() => Interlocked.Increment(ref _timeoutsSeen);

    /// <summary>Отказ способом «изменение состояния»: узел жив, но сам объявляет себя FAILED.</summary>
    public void MarkFailed(string reason)
    {
        _status = NodeStatus.Failed;
        _fault = NodeFault.State;
        _reason = reason;
        _role = NodeRole.Follower;
        _lastActivity = $"marked FAILED ({reason})";

        journal.Failure(Id, $"{Name} → FAILED ({reason})");
    }

    /// <summary>Отказ обнаружил кластер: состояние меняется на FAILED, но способ отказа остаётся прежним.</summary>
    public void DeclareFailed(string reason)
    {
        _status = NodeStatus.Failed;
        _role = NodeRole.Follower;
        _reason = reason;
        _lastActivity = $"declared FAILED ({reason})";
    }

    public void ApplyFault(NodeFault fault, string reason)
    {
        _fault = fault;
        _reason = reason;

        if (fault == NodeFault.State)
            _status = NodeStatus.Failed;

        _lastActivity = $"fault {fault} ({reason})";
        journal.Node(Id, $"{Name} → fault {fault.ToString().ToUpperInvariant()} ({reason})");
    }

    public void Restore(string reason)
    {
        _fault = NodeFault.None;
        _status = NodeStatus.Available;
        _role = NodeRole.Follower;
        _reason = reason;
        _lastActivity = $"restored ({reason})";

        journal.Node(Id, $"{Name} → AVAILABLE again ({reason})");
    }

    /// <summary>Смену ролей в журнал пишет кластер: узел только меняет своё состояние.</summary>
    public void BecomeLeader()
    {
        _role = NodeRole.Leader;
        _lastLeaderSeen = Name;
        _lastActivity = "elected as LEADER";
    }

    public void BecomeFollower(string leaderName)
    {
        _role = NodeRole.Follower;
        _lastLeaderSeen = leaderName;
        _lastActivity = $"follows {leaderName}";
    }

    public ClusterMessage? Handle(ClusterMessage incoming)
    {
        if (_fault == NodeFault.Exception)
        {
            Interlocked.Increment(ref _handled);
            _lastActivity = $"raised an exception for {incoming.Type}";
            journal.Failure(Id, $"{Name} raised an exception while handling {incoming.Type}: {_reason}");

            throw new NodeProcessingException(Id, incoming.Type, _reason);
        }

        string type = MessageTypes.Normalize(incoming.Type);

        bool silenced = _fault == NodeFault.Stalled
                        || (_fault == NodeFault.Silent
                            && (type == MessageTypes.Heartbeat || type == MessageTypes.Election));

        if (silenced)
        {
            Interlocked.Increment(ref _dropped);
            _lastActivity = $"dropped {type} (no response)";
            journal.Failure(Id, $"{Name} got {type} from Node {incoming.From} → NO RESPONSE");

            return null;
        }

        Interlocked.Increment(ref _handled);
        _lastActivity = $"handled {type} from Node {incoming.From}";

        if (incoming.From > 0)
            RememberPeer(incoming.From, IsAvailable ? "AVAILABLE" : "FAILED");

        return type switch
        {
            MessageTypes.Heartbeat => AnswerHeartbeat(incoming),
            MessageTypes.Election => AnswerElection(incoming),
            MessageTypes.Leader => AnnounceLeader(incoming),
            MessageTypes.Propose => Vote(incoming),
            MessageTypes.Commit => Confirm(incoming),
            MessageTypes.Status => Report(incoming),
            _ => null
        };
    }

    private ClusterMessage AnswerHeartbeat(ClusterMessage incoming)
    {
        Interlocked.Increment(ref _heartbeatsAnswered);

        return incoming with
        {
            Type = MessageTypes.Alive,
            From = Id,
            To = incoming.From,
            Status = StateName(),
            Role = RoleName(),
            KnownPeers = KnownPeers,
            Details = IsAvailable
                ? $"{Name} ALIVE, role {RoleName()}, knows {KnownPeers} peer(s)"
                : $"{Name} is FAILED: {_reason}"
        };
    }

    private ClusterMessage AnswerElection(ClusterMessage incoming) =>
        incoming with
        {
            Type = MessageTypes.Election,
            From = Id,
            To = incoming.From,
            Status = StateName(),
            Role = RoleName(),
            KnownPeers = KnownPeers,
            Details = IsAvailable
                ? $"{Name} AVAILABLE, ready to take part in the election"
                : $"{Name} FAILED, excluded from the election ({_reason})"
        };

    private ClusterMessage AnnounceLeader(ClusterMessage incoming)
    {
        string leader = incoming.Value;

        if (leader.Equals(Name, StringComparison.OrdinalIgnoreCase))
            BecomeLeader();
        else
            BecomeFollower(leader);

        return incoming with
        {
            From = Id,
            To = incoming.From,
            Status = StateName(),
            Details = $"{Name} accepted {leader} as the only leader",
            Role = RoleName()
        };
    }

    private ClusterMessage Vote(ClusterMessage incoming)
    {
        Interlocked.Increment(ref _votesCast);

        string vote;
        string details;

        if (!IsAvailable)
        {
            vote = VoteResults.Refused;
            details = $"{Name} is FAILED and does not vote (fault = {_fault})";
        }
        else if (incoming.Oppose.Contains(Id))
        {
            vote = VoteResults.No;
            details = $"{Name} disagrees with {incoming.Subject}={incoming.Value}";
        }
        else
        {
            vote = VoteResults.Yes;
            details = $"{Name} accepts {incoming.Subject}={incoming.Value}";
        }

        return incoming with
        {
            From = Id,
            To = incoming.From,
            Vote = vote,
            Status = StateName(),
            Role = RoleName(),
            Details = details
        };
    }

    private ClusterMessage Confirm(ClusterMessage incoming)
    {
        _agreedValue = incoming.Details.Length > 0 ? incoming.Details : $"{incoming.Subject}={incoming.Value}";
        _lastActivity = $"committed {incoming.Subject}={incoming.Value}";

        return incoming with
        {
            From = Id,
            To = incoming.From,
            Status = StateName(),
            Details = $"{Name} stored the agreed {incoming.Subject}={incoming.Value}"
        };
    }

    private ClusterMessage Report(ClusterMessage incoming) =>
        incoming with
        {
            Type = MessageTypes.Status,
            From = Id,
            To = incoming.From,
            Status = StateName(),
            Role = RoleName(),
            KnownPeers = KnownPeers,
            Details = $"{Name}: {StateName()}, role {RoleName()}, last activity - {_lastActivity}"
        };

    public string StateName() => IsAvailable ? "AVAILABLE" : "FAILED";

    public string RoleName() => Role == NodeRole.Leader ? "LEADER" : "FOLLOWER";

    public NodeReport Report() => new(
        Id, Name, Port, StateName(), RoleName(), _fault.ToString().ToUpperInvariant(), _reason,
        MessagesSent, MessagesHandled, HeartbeatsAnswered, VotesCast, TimeoutsSeen, KnownPeers, Peers,
        _lastLeaderSeen, _agreedValue, _lastActivity);
}

/// <summary>Искусственное исключение при обработке сообщения - четвёртый способ отказа из методички.</summary>
public sealed class NodeProcessingException(int nodeId, string messageType, string reason)
    : Exception($"node {nodeId} failed while handling {messageType}: {reason}");
