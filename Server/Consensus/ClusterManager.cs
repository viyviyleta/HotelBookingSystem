namespace Server.Consensus;

public sealed record ClusterResult(bool IsSuccess, string Message, object? Report = null);

public sealed class ClusterManager : IDisposable
{
    private readonly Dictionary<int, ClusterNodeHost> _hosts = [];
    private readonly object _gate = new();
    private readonly SemaphoreSlim _round = new(1, 1);

    private int _proposalCounter;
    private int _elections;
    private int _proposals;
    private int _achieved;
    private int _refused;
    private int _heartbeats;
    private int _timeouts;

    public ClusterManager(IReadOnlyList<ClusterNode> nodes, ClusterJournal journal, int timeoutMs = 1000)
    {
        Nodes = nodes;
        Journal = journal;
        TimeoutMs = timeoutMs;
    }

    public ClusterJournal Journal { get; }

    public IReadOnlyList<ClusterNode> Nodes { get; }

    public int Total => Nodes.Count;

    public int TimeoutMs { get; private set; }

    public bool IsRunning { get; private set; }

    public int LeaderId { get; private set; }

    public int LastLeaderId { get; private set; }

    public string LeaderName => LeaderId > 0 ? $"Node {LeaderId}" : "none";

    public string ElectionReason { get; private set; } = "not elected yet";

    public int Majority => MajorityOf(Total);

    public static int MajorityOf(int total) => total / 2 + 1;

    public int AvailableCount
    {
        get
        {
            lock (_gate)
                return Nodes.Count(node => node.IsAvailable && IsListening(node));
        }
    }

    // ------------------------------------------------------------------ ЗАПУСК


    public static ClusterManager CreateDefault(string clientId, int nodeCount, int basePort, int timeoutMs)
    {
        var journal = new ClusterJournal();
        var nodes = Enumerable.Range(1, nodeCount).Select(id => new ClusterNode(id, journal)).ToList();
        var cluster = new ClusterManager(nodes, journal, timeoutMs);

        cluster.Start(clientId, basePort);
        cluster.Elect("server start", clientId);

        return cluster;
    }

    public void Start(string clientId, int basePort)
    {
        lock (_gate)
        {
            try
            {
                foreach (ClusterNode node in Nodes)
                {
                    var host = new ClusterNodeHost(node, Journal);
                    _hosts[node.Id] = host;
                    host.Start(basePort + node.Id - 1);
                }
            }
            catch
            {
                // Порт занят: поднятые слушатели закрываем, чтобы кластер не оставался наполовину живым.
                foreach (ClusterNodeHost host in _hosts.Values)
                    host.Dispose();

                _hosts.Clear();
                throw;
            }

            foreach (ClusterNode node in Nodes)
                foreach (ClusterNode other in Nodes)
                    if (other != node)
                        node.RememberPeer(other.Id, other.StateName());

            IsRunning = true;
        }

        Journal.Cluster($"cluster is up: {Total} node(s) on ports {basePort}-{basePort + Total - 1}, " +
                        $"majority = {Majority} of {Total}, heartbeat timeout = {TimeoutMs} ms");
    }

    public void StartOnFreePorts(string clientId)
    {
        lock (_gate)
        {
            try
            {
                foreach (ClusterNode node in Nodes)
                {
                    var host = new ClusterNodeHost(node, Journal);
                    _hosts[node.Id] = host;
                    host.Start(0);
                }
            }
            catch
            {
                foreach (ClusterNodeHost host in _hosts.Values)
                    host.Dispose();

                _hosts.Clear();
                throw;
            }

            IsRunning = true;
        }

        Journal.Cluster($"cluster is up: {Total} node(s) on free ports " +
                        $"{string.Join(", ", Nodes.Select(node => node.Port))}, majority = {Majority} of {Total}");
    }

    public void Stop()
    {
        lock (_gate)
        {
            foreach (ClusterNodeHost host in _hosts.Values)
                host.Dispose();

            _hosts.Clear();
            IsRunning = false;
        }

        Journal.Cluster("cluster is stopped");
    }

    public void Dispose() => Stop();

    public void SetTimeout(int timeoutMs) => TimeoutMs = Math.Max(50, timeoutMs);

    private ClusterNodeHost? HostOf(ClusterNode node)
    {
        lock (_gate)
            return _hosts.TryGetValue(node.Id, out ClusterNodeHost? host) ? host : null;
    }

    private bool IsListening(ClusterNode node) => HostOf(node)?.IsListening == true;

    private ClusterNode? Find(int id) => Nodes.FirstOrDefault(node => node.Id == id);

    private void ClearLeader()
    {
        if (LeaderId > 0)
            LastLeaderId = LeaderId;

        LeaderId = 0;
    }

    // ------------------------------------------------------------------ СООБЩЕНИЯ МЕЖДУ УЗЛАМИ

    private NodeReply Ask(ClusterNode from, ClusterNode to, string type, string roundId = "",
        Proposal? proposal = null, string value = "", string reason = "")
    {
        var message = new ClusterMessage
        {
            Type = type,
            From = from.Id,
            To = to.Id,
            Proposal = roundId,
            Subject = proposal?.Subject ?? string.Empty,
            Value = value.Length > 0 ? value : proposal?.Value ?? string.Empty,
            EntityId = proposal?.EntityId ?? 0,
            Oppose = proposal?.Oppose ?? [],
            Reason = reason
        };

        if (!IsListening(to))
            return new NodeReply(to.Id, to.Name, type, "UNREACHABLE", "FAILED", to.RoleName(), 0,
                $"{to.Name} is offline: its listener is stopped", $"{from.Name} → {to.Name} (tcp refused)");

        try
        {
            NodeReply reply = NodeChannel.Request(from, to.Id, to.Port, message, TimeoutMs);

            return reply with { Connection = $"{from.Name} → {to.Name} ({reply.Connection})" };
        }
        catch (NodeTimeoutException ex)
        {
            Journal.Failure(to.Id, $"{from.Name} → HEARTBEAT → {to.Name} → NO RESPONSE ({ex.Message})");

            return new NodeReply(to.Id, to.Name, type, VoteResults.NoResponse, "FAILED", to.RoleName(),
                TimeoutMs, ex.Message, $"{from.Name} → {to.Name} (connected, no answer)");
        }
        catch (NodeUnreachableException ex)
        {
            Journal.Failure(to.Id, $"{from.Name} → {type} → {to.Name} → UNREACHABLE ({ex.Message})");

            return new NodeReply(to.Id, to.Name, type, "UNREACHABLE", "FAILED", to.RoleName(), 0, ex.Message,
                $"{from.Name} → {to.Name} (tcp refused)");
        }
    }
    private IReadOnlyList<NodeReply> AskAll(ClusterNode from, IEnumerable<ClusterNode> targets, string type,
        string roundId = "", Proposal? proposal = null, string value = "", string reason = "")
    {
        List<Task<NodeReply>> pending = targets
            .Select(target => Task.Run(() => Ask(from, target, type, roundId, proposal, value, reason)))
            .ToList();

        return Task.WhenAll(pending).GetAwaiter().GetResult().OrderBy(reply => reply.NodeId).ToList();
    }

    private static bool SaysAvailable(NodeReply reply) =>
        reply.Status == "AVAILABLE" && reply.Result is not (VoteResults.NoResponse or "UNREACHABLE");

    // ------------------------------------------------------------------ ЧАСТЬ 3: HEARTBEAT

    public HeartbeatReport Probe(string clientId)
    {
        _round.Wait();

        try
        {
            List<NodeReply> exchanges = [];
            List<string> down = [];
            List<string> detected = [];

            ClusterNode? leader = Find(LeaderId);

            if (leader is not null)
            {
                // Лидер опрашивает фолловеров...
                exchanges.AddRange(AskAll(leader, Nodes.Where(node => node.Id != leader.Id),
                    MessageTypes.Heartbeat));

                // ...и сам лидер проверяется фолловером
                ClusterNode? checker = Nodes.Where(node => node.Id != leader.Id && node.IsAvailable)
                    .OrderBy(node => node.Id)
                    .FirstOrDefault();

                if (checker is not null)
                {
                    Journal.Heartbeat(checker.Id, $"{checker.Name} → HEARTBEAT → {leader.Name}");

                    NodeReply answer = Ask(checker, leader, MessageTypes.Heartbeat);
                    exchanges.Add(answer with { Connection = $"{checker.Name} → {leader.Name} (leader check)" });
                }
            }
            else
            {
                // Лидера нет: опрос ведёт самый младший узел, а его самого проверяет тот, кто ответил.
                ClusterNode instigator = Nodes.OrderBy(node => node.Id).First();

                exchanges.AddRange(AskAll(instigator, Nodes.Where(node => node.Id != instigator.Id),
                    MessageTypes.Heartbeat));

                ClusterNode? responder = exchanges.Where(SaysAvailable)
                    .OrderByDescending(reply => reply.NodeId)
                    .Select(reply => Find(reply.NodeId))
                    .FirstOrDefault(node => node is not null);

                if (responder is not null)
                {
                    NodeReply answer = Ask(responder, instigator, MessageTypes.Heartbeat);
                    exchanges.Add(answer with { Connection = $"{responder.Name} → {instigator.Name} (reverse check)" });
                }
            }

            Interlocked.Add(ref _heartbeats, exchanges.Count);

            foreach (NodeReply reply in exchanges.Where(each => !SaysAvailable(each)))
            {
                ClusterNode? node = Find(reply.NodeId);

                if (node is null)
                    continue;

                if (!node.IsAvailable)
                {
                    down.Add(node.Name);
                    detected.Add($"{node.Name} already {node.StateName()}");
                    continue;
                }

                Journal.Failure(node.Id, $"{node.Name} → " +
                    (reply.Result == VoteResults.NoResponse ? "HEARTBEAT TIMEOUT" : "NO VALID ANSWER"));
                node.DeclareFailed($"{reply.Result.ToLowerInvariant()}: {reply.Details}");
                Journal.Failure(node.Id, $"{node.Name} → FAILED");
                down.Add(node.Name);
                detected.Add($"{node.Name} {reply.Result}");

                if (node.Id == LeaderId)
                {
                    Interlocked.Increment(ref _timeouts);
                    ClearLeader();
                    Journal.Failure(node.Id, $"{node.Name} was LEADER → {reply.Result.ToLowerInvariant()} → " +
                                             "the cluster needs a new election");
                }
            }

            foreach (NodeReply reply in exchanges.Where(SaysAvailable))
                Journal.Heartbeat(reply.NodeId, $"{reply.Connection} → {reply.Result} " +
                                                 $"({reply.RoundTripMs} ms, role {reply.Role}, knows {reply.KnownPeers} peer(s))");

            string summary = $"HEARTBEAT round: exchanges = {exchanges.Count}, answered = " +
                             $"{exchanges.Count(SaysAvailable)}, failures detected = {detected.Count}" +
                             (detected.Count > 0
                                 ? $" → {string.Join(", ", detected)}" +
                                   (LeaderId == 0 ? ", the cluster has no leader now" : string.Empty)
                                 : " → every node is alive");

            Journal.Heartbeat(leader?.Id ?? Nodes.OrderBy(node => node.Id).First().Id, summary);

            return new HeartbeatReport(summary, (leader ?? Nodes.First()).Name, exchanges, down, Journal.Recent(14));
        }
        finally
        {
            _round.Release();
        }
    }

    // ------------------------------------------------------------------ ЧАСТИ 2 И 4: ВЫБОРЫ ЛИДЕРА


    public ElectionReport Elect(string reason, string clientId)
    {
        _round.Wait();

        try
        {
            Interlocked.Increment(ref _elections);
            int previous = LeaderId > 0 ? LeaderId : LastLeaderId;

            Journal.Election("CLUSTER", $"Election started (reason: {reason})");

            ClusterNode instigator = Nodes.Where(node => node.IsAvailable)
                .OrderBy(node => node.Id)
                .FirstOrDefault() ?? Nodes[0];

            List<NodeReply> polls = [.. AskAll(instigator, Nodes.Where(node => node.Id != instigator.Id),
                MessageTypes.Election, reason: reason)];

            // О себе инциатору судить не нужно: его состояние известно локально.
            polls.Add(new NodeReply(instigator.Id, instigator.Name, MessageTypes.Election, instigator.StateName(),
                instigator.StateName(), instigator.RoleName(), 0, $"{instigator.Name} started the election",
                "local state"));

            polls = [.. polls.OrderBy(reply => reply.NodeId)];

            List<string> excluded = [];

            foreach (NodeReply reply in polls)
            {
                ClusterNode? node = Find(reply.NodeId);

                if (node is null)
                    continue;

                instigator.RememberPeer(node.Id, reply.Status);

                if (SaysAvailable(reply))
                {
                    if (!node.IsAvailable)
                        node.Restore($"answered the election poll: {reply.Details}");

                    Journal.Election(node.Id, $"{node.Name} AVAILABLE (role {reply.Role}, " +
                                              $"knows {node.KnownPeers} peer(s))");
                }
                else
                {
                    if (node.IsAvailable)
                        node.DeclareFailed($"excluded from the election: {reply.Result.ToLowerInvariant()}");

                    excluded.Add(node.Name);
                    Journal.Election(node.Id, $"{node.Name} {reply.Result} → excluded ({reply.Details})");
                }
            }

            List<ClusterNode> candidates = polls.Where(SaysAvailable)
                .Select(reply => Find(reply.NodeId))
                .Where(node => node is not null)
                .Select(node => node!)
                .ToList();

            ClusterNode? winner = candidates.OrderByDescending(node => node.Id).FirstOrDefault();

            if (winner is null)
            {
                ClearLeader();
                ElectionReason = reason;
                Journal.Election("CLUSTER", $"no available node left ({candidates.Count} of {Total}) → " +
                                            "the cluster has no leader and suspends decisions");

                return ElectionOutcome(reason, instigator, previous, 0, polls, excluded, false);
            }

            winner.BecomeLeader();
            LeaderId = winner.Id;
            LastLeaderId = winner.Id;
            ElectionReason = reason;

            Journal.Election(winner.Id, $"{winner.Name} elected as LEADER " +
                                       $"(max id among {candidates.Count} available node(s), majority = {Majority} of {Total})");

            // Лидер объявляет себя остальным настоящим сообщением, а не молча меняя их роль.
            foreach (NodeReply announce in AskAll(winner,
                         Nodes.Where(node => node.Id != winner.Id && node.IsAvailable),
                         MessageTypes.Leader, value: winner.Name))
            {
                if (SaysAvailable(announce))
                    Journal.Role(announce.NodeId, announce.Details);
            }

            foreach (ClusterNode node in Nodes)
                winner.RememberPeer(node.Id, node.StateName());

            int leaders = Nodes.Count(node => node.Role == NodeRole.Leader && node.IsAvailable);

            if (leaders != 1)
            {
                // Требование Части 4: лидер должен быть ровно один - лишних снимаем с роли.
                foreach (ClusterNode stray in Nodes.Where(node => node.Id != winner.Id
                                                  && node.Role == NodeRole.Leader))
                    stray.BecomeFollower(winner.Name);

                Journal.Error(winner.Id, $"the cluster saw {leaders} leaders - every node except {winner.Name} " +
                                         "was set back to FOLLOWER, so exactly one leader remains");
            }

            return ElectionOutcome(reason, instigator, previous, winner.Id, polls, excluded, leaders == 1);
        }
        finally
        {
            _round.Release();
        }
    }

    private ElectionReport ElectionOutcome(string reason, ClusterNode instigator, int previous, int winner,
        IReadOnlyList<NodeReply> polls, IReadOnlyList<string> excluded, bool singleLeader)
    {
        int available = polls.Count(SaysAvailable);

        string summary = winner > 0
            ? $"{(previous > 0 && previous != winner ? "re-elected" : "elected")}: Node {winner} is LEADER ({reason}), " +
              $"available = {available}/{Total}, majority = {Majority}, single leader = {(singleLeader ? "yes" : "no")}" +
              (excluded.Count > 0 ? $", excluded = {string.Join(", ", excluded)}" : string.Empty)
            : $"no leader elected ({reason}): available = {available}/{Total}, majority = {Majority}";

        return new ElectionReport(summary, reason, instigator.Name, previous, winner,
            winner > 0 ? $"Node {winner}" : "none", Majority, available, Total, singleLeader, 0,
            polls, excluded, Journal.Recent(16));
    }

    // ------------------------------------------------------------------ ЧАСТЬ 5: ПРЕДЛОЖЕНИЕ И ГОЛОСА

    public ConsensusReport Propose(Proposal proposal, string clientId, Func<Proposal, AppliedDecision>? apply = null)
    {
        _round.Wait();

        try
        {
            Interlocked.Increment(ref _proposals);
            string round = $"P{Interlocked.Increment(ref _proposalCounter)}";

            ClusterNode? leader = Find(LeaderId);

            if (leader is null || !leader.IsAvailable || !IsListening(leader))
            {
                Interlocked.Increment(ref _refused);
                Journal.Consensus("CLUSTER", $"PROPOSE {proposal.Description} refused: {LeaderName} is not an " +
                                             "available leader, run an election first");

                return new ConsensusReport($"NOT ACHIEVED: {proposal.Description} → there is no available leader, " +
                                           "so nobody can coordinate the vote", round, proposal.Description, "none",
                    Majority, AvailableCount, Total, 0, 0, 0, 0, false, false, "NOT ACHIEVED", string.Empty,
                    string.Empty, 0, [], Journal.Recent(8));
            }

            Journal.Consensus(leader.Id, $"Leader {leader.Name} → PROPOSE {proposal.Description} ({round})");

            List<NodeReply> votes = [.. AskAll(leader, Nodes.Where(node => node.Id != leader.Id),
                MessageTypes.Propose, round, proposal)];

            // Голос самого лидера считается вместе с остальными
            bool leaderOpposed = proposal.Oppose?.Contains(leader.Id) == true;
            string leaderVote = leaderOpposed ? VoteResults.No : VoteResults.Yes;

            votes.Add(new NodeReply(leader.Id, leader.Name, MessageTypes.Vote, leaderVote, leader.StateName(),
                leader.RoleName(), 0,
                leaderOpposed ? $"{leader.Name} disagrees with the proposal" : $"{leader.Name} proposed and votes YES",
                $"{leader.Name} → itself"));

            votes = [.. votes.OrderBy(reply => reply.NodeId)];

            int yes = votes.Count(reply => reply.Result == VoteResults.Yes);
            int no = votes.Count(reply => reply.Result == VoteResults.No);
            int noResponse = votes.Count(reply => reply.Result is VoteResults.NoResponse or "UNREACHABLE");
            int refusedVotes = votes.Count(reply => reply.Result == VoteResults.Refused);

            foreach (NodeReply reply in votes)
                Journal.Vote(reply.NodeId, $"{reply.Connection} → {reply.Result} ({reply.RoundTripMs} ms, " +
                                           $"{round}: {reply.Details})");

            Interlocked.Add(ref _timeouts, noResponse + refusedVotes);

            Journal.Vote(leader.Id, $"YES = {yes}, NO = {no}, NO RESPONSE = {noResponse}, REFUSED = {refusedVotes}");
            Journal.Consensus(leader.Id, $"Majority = {Majority} (nodes that answered = {yes + no} of {Total})");

            AppliedDecision applied = new(false, string.Empty);

            if (yes >= Majority)
            {
                Interlocked.Increment(ref _achieved);
                Journal.Consensus(leader.Id, "CONSENSUS ACHIEVED");

                applied = ApplyDecision(leader, proposal, apply);

                foreach (NodeReply commit in AskAll(leader,
                             Nodes.Where(node => node.Id != leader.Id && node.IsAvailable),
                             MessageTypes.Commit, round, proposal,
                             reason: $"COMMIT {proposal.Subject}={proposal.Value}"))
                {
                    if (SaysAvailable(commit))
                        Journal.Commit(commit.NodeId, commit.Details);
                }
            }
            else
            {
                Interlocked.Increment(ref _refused);
                Journal.Consensus(leader.Id, "CONSENSUS NOT ACHIEVED → the value is not written anywhere, " +
                                             "the domain data stay unchanged");
            }

            string summary = (yes >= Majority ? "CONSENSUS ACHIEVED" : "CONSENSUS NOT ACHIEVED") +
                             $": {proposal.Description} proposed by {leader.Name} → YES = {yes}, NO = {no}, " +
                             $"NO RESPONSE = {noResponse}, REFUSED = {refusedVotes}, Majority = {Majority} of {Total}";

            return new ConsensusReport(summary, round, proposal.Description, leader.Name, Majority,
                yes + no, Total, yes, no, noResponse, refusedVotes, yes >= Majority, applied.Applied,
                yes >= Majority ? "ACHIEVED" : "NOT ACHIEVED", applied.Note, applied.EventName,
                votes.Count > 0 ? votes.Max(reply => reply.RoundTripMs) : 0, votes, Journal.Recent(18));
        }
        finally
        {
            _round.Release();
        }
    }

    private AppliedDecision ApplyDecision(ClusterNode leader, Proposal proposal, Func<Proposal, AppliedDecision>? apply)
    {
        if (apply is null)
        {
            Journal.Domain(leader.Id, $"the agreed {proposal.Subject}={proposal.Value} has nothing to apply in this mode");
            return new AppliedDecision(false, $"agreed by {Majority} node(s), no domain action requested");
        }

        try
        {
            AppliedDecision applied = apply(proposal);

            if (applied.Note.Length > 0)
                Journal.Domain(leader.Id, applied.Note);

            return applied;
        }
        catch (Exception ex)
        {
            // Большинство проголосовало, но применить значение не удалось - это отдельная фиксируемая ситуация.
            Journal.Error(leader.Id, $"the agreed value could not be applied: {ex.GetType().Name}: {ex.Message}");
            return new AppliedDecision(false, $"consensus achieved, applying failed: {ex.Message}");
        }
    }

    // ------------------------------------------------------------------ ОТКАЗ И ВОССТАНОВЛЕНИЕ УЗЛА

    public ClusterResult FailNode(int id, string mode, string reason, string clientId)
    {
        ClusterNode? node = Find(id);

        if (node is null)
            return Refuse($"Unknown node id '{id}'. Cluster nodes: {string.Join(", ", Nodes.Select(each => each.Name))}");

        NodeFault fault = ParseFault(mode, out string? error);

        if (error is not null)
            return Refuse(error);

        if (node.Fault == fault && !node.IsAvailable)
            return Refuse($"{node.Name} is already {node.StateName()} (fault = {Describe(fault)})");

        if (fault == NodeFault.Offline)
            HostOf(node)?.Stop();

        if (fault == NodeFault.State)
            node.MarkFailed(Reason(reason, fault));
        else
            node.ApplyFault(fault, Reason(reason, fault));

        if (node.Id == LeaderId && fault == NodeFault.State)
        {
            ClearLeader();
            Journal.Failure(node.Id, $"{node.Name} was LEADER → the cluster waits for a new election");
        }

        return new ClusterResult(true, $"{node.Name} → {Describe(fault)} ({node.Reason}), " +
                                       $"available = {AvailableCount}/{Total}, majority = {Majority}" +
                                       (node.IsAvailable
                                           ? "; the cluster has not noticed it yet - a heartbeat round or an election will"
                                           : ", consensus is " +
                                             (AvailableCount >= Majority ? "still reachable" : "impossible until nodes return")),
            Status());
    }

    public ClusterResult RestoreNode(int id, string reason, string clientId)
    {
        ClusterNode? node = Find(id);

        if (node is null)
            return Refuse($"Unknown node id '{id}'. Cluster nodes: {string.Join(", ", Nodes.Select(each => each.Name))}");

        HostOf(node)?.Start(node.Port);
        node.Restore(reason.Length > 0 ? reason : "node is back and joins as FOLLOWER");

        return new ClusterResult(true, $"{node.Name} → AVAILABLE (role {node.RoleName()}, " +
                                       $"available = {AvailableCount}/{Total}; an election can let it take the lead again)",
            Status());
    }

    private static string Reason(string reason, NodeFault fault) =>
        reason.Length > 0 ? reason : DefaultReason(fault);

    private static string Describe(NodeFault fault) => fault.ToString().ToUpperInvariant();

    private static NodeFault ParseFault(string mode, out string? error)
    {
        error = null;

        return mode.Trim().ToLowerInvariant() switch
        {
            "" or "state" or "unavailable" => NodeFault.State,
            "stalled" or "stopped" => NodeFault.Stalled,
            "exception" => NodeFault.Exception,
            "silent" or "heartbeat" => NodeFault.Silent,
            "offline" or "crashed" => NodeFault.Offline,
            _ => UnknownFault(out error, mode)
        };
    }

    private static NodeFault UnknownFault(out string? error, string mode)
    {
        error = $"Unknown failure mode '{mode}'. Supported ways to model a failure: " +
                "state (change of state), stalled (stopped processing), exception (artificial exception), " +
                "silent (no heartbeat answers), offline (node is down)";

        return NodeFault.None;
    }

    private static string DefaultReason(NodeFault fault) => fault switch
    {
        NodeFault.State => "node state changed to FAILED",
        NodeFault.Stalled => "node stopped processing messages",
        NodeFault.Exception => "artificial exception while handling a message",
        NodeFault.Silent => "node stopped answering heartbeats",
        NodeFault.Offline => "node process is down, listener stopped",
        _ => "running"
    };

    private static ClusterResult Refuse(string message) => new(false, message);

    // ------------------------------------------------------------------ РУЧНЫЕ СООБЩЕНИЯ И ОТЧЁТЫ

    public ClusterResult SendMessage(int fromId, int toId, string type, string clientId)
    {
        ClusterNode? from = Find(fromId);
        ClusterNode? to = Find(toId);

        if (from is null || to is null)
            return Refuse($"Unknown node in the message: from = {fromId}, to = {toId}. " +
                          $"Cluster nodes: {string.Join(", ", Nodes.Select(each => each.Name))}");

        if (!MessageTypes.IsKnown(type))
            return Refuse($"Unknown message type '{type}'. Known types: {string.Join(", ", MessageTypes.Known)}");

        if (from.Id == to.Id)
            return Refuse($"{from.Name} cannot message itself: choose two different nodes");

        NodeReply reply = Ask(from, to, MessageTypes.Normalize(type));

        Journal.Heartbeat(from.Id, $"{from.Name} → {to.Name} ({MessageTypes.Normalize(type)}) → " +
                                   $"{reply.Result} ({reply.RoundTripMs} ms, {reply.Details})");

        return SaysAvailable(reply)
            ? new ClusterResult(true, $"{from.Name} → {reply.Result} → {to.Name}: {reply.Details}", Status())
            : new ClusterResult(false, $"{from.Name} → {reply.Result} → {to.Name}: {reply.Details}", Status());
    }


    public ClusterResult Broadcast(string type, string clientId)
    {
        if (!MessageTypes.IsRequest(type))
            return Refuse($"Cannot broadcast '{type}': only request types can be sent, " +
                          $"accepted requests = {string.Join(", ", MessageTypes.Requests)}");

        ClusterNode from = Find(LeaderId) ?? Nodes.Where(node => node.IsAvailable).MaxBy(node => node.Id)!;
        IReadOnlyList<NodeReply> replies = AskAll(from, Nodes.Where(node => node.Id != from.Id),
            MessageTypes.Normalize(type));

        string answers = string.Join(", ", replies.Select(reply => $"{reply.Node} = {reply.Result}"));

        Journal.Heartbeat(from.Id, $"{from.Name} → {MessageTypes.Normalize(type)} → all peers at once: {answers}");

        return new ClusterResult(true, $"parallel {MessageTypes.Normalize(type)} from {from.Name} to " +
                                       $"{replies.Count} node(s): {answers}", Status());
    }

    public ClusterStatusReport Status()
    {
        IReadOnlyList<NodeReport> nodes = Nodes.Select(node => node.Report()).ToList();
        int first = nodes.Min(node => node.Port);
        int last = nodes.Max(node => node.Port);

        string ports = first > 0 ? $"ports {first}-{last}" : "ports are not bound yet";

        string summary = $"nodes = {Total} ({ports}), leader = {LeaderName} ({ElectionReason}), " +
                         $"available = {AvailableCount}/{Total}, majority = {Majority}, elections = {_elections}, " +
                         $"proposals = {_proposals}, achieved = {_achieved}, not achieved = {_refused}, " +
                         $"heartbeats = {_heartbeats}, timeouts = {_timeouts}";

        return new ClusterStatusReport(true, summary, Total, Majority, AvailableCount, LeaderName, ElectionReason,
            _elections, _proposals, _achieved, _refused, _heartbeats, _timeouts, nodes);
    }

    public ClusterStatisticsReport Statistics(int lines) =>
        new($"cluster of {Total} node(s): leader = {LeaderName}, majority = {Majority}, elections = {_elections}, " +
            $"proposals = {_proposals}, achieved = {_achieved}, not achieved = {_refused}",
            Total, Majority, _elections, LeaderName, _proposals, _achieved, _refused, _heartbeats, _timeouts,
            Nodes.Sum(node => node.MessagesSent), Nodes.Sum(node => node.MessagesHandled),
            Nodes.Sum(node => node.VotesCast), Nodes.Select(node => node.Report()).ToList(), Journal.Recent(lines));

    public IReadOnlyList<JournalEntry> RecentJournal(int lines) => Journal.Recent(lines);

    public IReadOnlyList<ClusterNode> AvailableNodes => Nodes.Where(node => node.IsAvailable)
        .OrderBy(node => node.Id).ToList();
}
