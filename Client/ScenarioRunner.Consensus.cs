using Shared.Network;

namespace Client;

/// <summary>
/// Сценарии ЛР №7: узлы, выбор лидера, обнаружение отказа, повторный выбор, голосование и консенсус.
/// Клиент остаётся клиентом предыдущих работ: он выполняет те же операции предметной области, но
/// теперь запись состояния идёт только после согласия большинства узлов.
/// </summary>
public sealed partial class ScenarioRunner
{
    private static readonly string[] NodeHeader =
        ["Узел", "Порт TCP", "Состояние", "Роль", "Отказ", "Причина", "Знает узлов", "Отправлено",
            "Принято", "Ответов HB", "Голосов", "Таймаутов", "Лидером видит", "Согласовано"];

    private static readonly string[] ExchangeHeader =
        ["Обмен", "Сообщение", "Итог", "Состояние", "Роль", "Ответ, мс", "Знает узлов", "Комментарий"];

    private static readonly string[] VoteHeader =
        ["№", "Узел", "Голос", "Состояние", "Роль", "Ответ, мс", "Основание"];

    private static readonly string[] JournalHeader = ["Время", "Кто", "Метка", "Событие"];

    private static readonly string[] ExperimentHeader =
        ["Эксперимент", "Доступно", "Большинство", "Лидер", "YES", "NO", "NO RESP", "REFUSED",
            "Решение кластера", "Ожидалось", "Совпадает"];

    // ------------------------------------------------------------------ ПРОВЕРКА РЕЖИМА

    /// <summary>Кластер поднимается флагом --cluster on: без него клиент получает понятный отказ.</summary>
    public bool EnsureConsensusMode()
    {
        ClusterStatusReport? report = Cluster();

        if (report is null)
        {
            ClientLog.Line("[LR7] the server runs without a node cluster.");
            ClientLog.Line("[LR7] start it with: --storage postgres --replication on --pubsub on --cluster on " +
                           "--nodes 5 --cluster-port 5401");
            return false;
        }

        ClientLog.Line($"[LR7] {report.Summary}");
        return true;
    }

    private ClusterStatusReport? Cluster() =>
        Read<ClusterStatusReport>(_client.SendRequest(RequestBuilder.BuildClusterStatus()));

    private ElectionReport? Elect(string reason) =>
        Read<ElectionReport>(_client.SendRequest(RequestBuilder.BuildStartElection(reason)));

    private HeartbeatReport? Probe() =>
        Read<HeartbeatReport>(_client.SendRequest(RequestBuilder.BuildHeartbeatProbe()));

    private ConsensusReport? VoteOf(Response response) => Read<ConsensusReport>(response);

    private ClusterStatisticsReport? Counters() =>
        Read<ClusterStatisticsReport>(_client.SendRequest(RequestBuilder.BuildClusterStatistics()));

    /// <summary>Узлы возвращаются в рабочее состояние, чтобы сценарии не наследовали отказы друг друга.</summary>
    private void RestoreAllNodes()
    {
        ClusterStatusReport? report = Cluster();

        if (report is null)
            return;

        foreach (NodeReport node in report.Nodes.Where(each => each.Status != "AVAILABLE" || each.Fault != "NONE"))
            _client.SendRequest(RequestBuilder.BuildRestoreNode(node.Id, "scenario reset"));

        ElectionReport? election = Elect("scenario reset: every node is back");

        ClientLog.Line($"[LR7] механизм приведён в рабочее состояние: {election?.Summary ?? "кластер недоступен"}");
    }

    // ------------------------------------------------------------------ ОТЧЁТ О СОСТОЯНИИ

    public void PrintNodeCluster()
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR7: NODES, ROLES AND LEADER ===");

        if (!EnsureConsensusMode())
            return;

        PrintNodes();
    }

    private void PrintNodes()
    {
        ClusterStatusReport? report = Cluster();

        if (report is null)
            return;

        PrintTable(NodeHeader, report.Nodes.Select(node => new[]
        {
            node.Name,
            node.Port.ToString(),
            node.Status,
            node.Role,
            node.Fault,
            Shorten(node.Reason),
            node.KnownPeers.ToString(),
            node.MessagesSent.ToString(),
            node.MessagesHandled.ToString(),
            node.HeartbeatsAnswered.ToString(),
            node.VotesCast.ToString(),
            node.TimeoutsSeen.ToString(),
            node.LastLeaderSeen,
            Shorten(node.AgreedValue)
        }).ToList());

        ClientLog.Line($"[LR7] leader = {report.Leader}, доступно = {report.Available}/{report.Total}, " +
                       $"большинство = {report.Majority}, выборов = {report.Elections}, " +
                       $"предложений = {report.Proposals}, консенсус достигнут = {report.ConsensusAchieved}, " +
                       $"не достигнут = {report.ConsensusRefused}");
    }

    private static void PrintExchanges(IReadOnlyList<NodeReply>? exchanges)
    {
        if (exchanges is null || exchanges.Count == 0)
        {
            ClientLog.Line("[LR7] обменов нет: сервер не вернул таблицу");
            return;
        }

        PrintTable(ExchangeHeader, exchanges.Select(reply => new[]
        {
            reply.Connection,
            reply.MessageType,
            reply.Result,
            reply.Status,
            reply.Role,
            reply.RoundTripMs.ToString(),
            reply.KnownPeers.ToString(),
            Shorten(reply.Details)
        }).ToList());
    }

    private static void PrintVotes(IReadOnlyList<NodeReply>? votes)
    {
        if (votes is null || votes.Count == 0)
        {
            ClientLog.Line("[LR7] голосов в ответе нет");
            return;
        }

        int number = 0;

        PrintTable(VoteHeader, votes.Select(reply => new[]
        {
            (number += 1).ToString(),
            reply.Node,
            reply.Result,
            reply.Status,
            reply.Role.Length > 0 ? reply.Role : "-",
            reply.RoundTripMs.ToString(),
            Shorten(reply.Details)
        }).ToList());
    }

    private static void PrintJournal(IReadOnlyList<JournalEntry>? lines)
    {
        if (lines is null || lines.Count == 0)
        {
            ClientLog.Line("[LR7] журнал кластера пуст");
            return;
        }

        PrintTable(JournalHeader, lines.Select(entry => new[]
        {
            entry.Time,
            entry.Who,
            entry.Marker,
            Shorten(entry.Text)
        }).ToList());
    }

    // ------------------------------------------------ ЧАСТИ 1-2: УЗЛЫ И ВЫБОР ЛИДЕРА

    /// <summary>Часть 1: пять узлов с ID, состоянием, ролью и таблицей соседей. Часть 2: выбор лидера.</summary>
    public bool RunClusterNodes(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7 PART 1-2: FIVE NODES AND LEADER ELECTION, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        RestoreAllNodes();

        ClientLog.Line("[1] Часть 1: узлы работают на своих TCP-портах, у каждого есть ID, состояние и роль");
        PrintNodes();

        ClusterStatusReport? before = Cluster();

        if (before is not null)
            foreach (NodeReport node in before.Nodes)
                ClientLog.Line($"    {node.Name}: ID = {node.Id}, состояние = {node.Status}, роль = {node.Role}, " +
                               $"знает других узлов = {node.KnownPeers} ({Join(node.Peers)})");

        ClientLog.Line($"[2] Часть 2: правило выбора - доступный узел с наибольшим ID; Majority = " +
                       $"{before?.Majority} из {before?.Total}");

        ElectionReport? election = Elect("Часть 2: первый выбор лидера");

        if (election is null)
            return false;

        ClientLog.Line($"[3] опрос доступности по TCP: {election.Initiator} разослал ELECTION-запросы, " +
                       $"ответов получено {election.Polls.Count} из {election.Total}");
        PrintExchanges(election.Polls);

        ClientLog.Line($"[4] результат: {election.Summary}");
        PrintNodes();

        ClientLog.Line($"[5] одновременно лидер один: {(election.SingleLeader ? "да" : "нет")}");
        ClientLog.Line("[6] часть журнала выборов:");
        PrintJournal(election.Journal);

        return true;
    }

    // ------------------------------------- ЧАСТИ 3-4: HEARTBEAT, ОТКАЗ ЛИДЕРА, ПОВТОРНЫЙ ВЫБОР

    /// <summary>Часть 3: heartbeat и обнаружение отказа. Часть 4: повторный выбор и единственный лидер.</summary>
    public bool RunClusterFailure(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7 PART 3-4: HEARTBEAT, LEADER FAILURE AND RE-ELECTION, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        RestoreAllNodes();

        ClusterStatusReport? start = Cluster();
        string leader = start?.Leader ?? "none";

        ClientLog.Line($"[1] текущий лидер = {leader}, доступно = {start?.Available}/{start?.Total}");

        HeartbeatReport? beforeFailure = Probe();
        ClientLog.Line($"[2] heartbeat до отказа → {beforeFailure?.Summary}");

        if (beforeFailure is not null)
            PrintExchanges(beforeFailure.Exchanges);

        int leaderId = NodeIdOf(leader) ?? start!.Total;

        ClientLog.Line($"[3] моделируем отказ лидера: {leader} перестаёт отвечать на heartbeat " +
                       $"(способ из методички, таймаут проверяет кластер, а не клиент)");
        Response fault = _client.SendRequest(RequestBuilder.BuildFailNode(leaderId, "silent",
            "the leader stopped answering heartbeats"));
        ClientLog.Line($"[4] сервер принял отказ → {Shorten(fault.Message)}");

        HeartbeatReport? afterFailure = Probe();
        ClientLog.Line($"[5] обнаружение отказа → {afterFailure?.Summary}");

        if (afterFailure is not null)
        {
            PrintExchanges(afterFailure.Exchanges);
            ClientLog.Line($"    отказанные узлы: {Join(afterFailure.FailedNodes)}");
        }

        ElectionReport? reelection = Elect($"Часть 4: повторный выбор после отказа {leader}");
        ClientLog.Line($"[6] {reelection?.Summary}");

        if (reelection is not null)
        {
            PrintNodes();
            ClientLog.Line($"[7] одновременно лидер один: {(reelection.SingleLeader ? "да" : "нет")} " +
                           $"(исключённые узлы: {Join(reelection.Excluded)})");
            ClientLog.Line("[8] часть журнала:");
            PrintJournal(reelection.Journal);
        }

        ClientLog.Line("[9] остальные способы отказа из методички: остановка обработки, искусственное исключение, " +
                       "выключенный узел, явная смена состояния");
        RestoreAllNodes();

        foreach ((int node, string mode, string why) in new[]
        {
            (1, "stalled", "node stopped processing messages"),
            (2, "exception", "artificial exception while handling a message"),
            (3, "offline", "node process is down, listener stopped"),
            (4, "state", "node state changed to FAILED")
        })
        {
            Response failed = _client.SendRequest(RequestBuilder.BuildFailNode(node, mode, why));
            HeartbeatReport? round = Probe();

            ClientLog.Line($"    mode {mode,-10} → {FirstSegment(failed.Message)} | проверка: " +
                           $"{Shorten(round?.Summary ?? "нет ответа")}");

            _client.SendRequest(RequestBuilder.BuildRestoreNode(node, "restored after the failure mode check"));
        }

        ElectionReport? restored = Elect("Часть 4: узлы вернулись, выборы заново");
        ClientLog.Line($"[10] {restored?.Summary}");
        PrintNodes();

        return true;
    }

    // ------------------------------------------------- ЧАСТЬ 5: ГОЛОСОВАНИЕ И КОНСЕНСУС

    /// <summary>Часть 5: лидер предлагает BookingStatus, узлы голосуют, большинство решает исход записи.</summary>
    public bool RunClusterVote(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7 PART 5: PROPOSE, VOTE AND MAJORITY, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        ResetDatabase();
        RestoreAllNodes();

        ClusterStatusReport? cluster = Cluster();
        int majority = cluster?.Majority ?? 3;

        ClientLog.Line($"[1] кластер: лидер = {cluster?.Leader}, доступно = {cluster?.Available}/{cluster?.Total}, " +
                       $"большинство = {majority}");

        int booking = Book(roomId);

        if (booking == 0)
            return false;

        ClientLog.Line($"[2] состояние брони {booking} до голосования: {BookingStatus(booking)}");

        Response proposed = _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Paid", booking));
        ConsensusReport? paid = VoteOf(proposed);

        ClientLog.Line($"[3] лидер предложил изменение → {proposed.Message.Split('|')[0]}");

        if (paid is not null)
        {
            PrintVotes(paid.Votes);
            ClientLog.Line($"[4] подсчёт голосов: YES = {paid.Yes}, NO = {paid.No}, NO RESPONSE = {paid.NoResponse}, " +
                           $"REFUSED = {paid.Refused}, Majority = {paid.Majority} → {paid.Decision}");
            ClientLog.Line($"[5] что сделал кластер после согласия: {Shorten(paid.AppliedNote)}");
            ClientLog.Line($"[6] состояние брони в базе после консенсуса: {BookingStatus(booking)}");
            PrintJournal(paid.Journal);
        }

        Response cancelled = _client.SendRequest(RequestBuilder.BuildCancelBooking(booking));
        ClientLog.Line($"[7] обычная операция клиента CancelBooking тоже идёт через голосование: " +
                       $"{cancelled.Message.Split('|')[0]} (Event = {cancelled.EventTriggered})");
        ClientLog.Line($"    решение кластера в ответе: {Shorten(ConsensusSegment(cancelled))}");
        ClientLog.Line($"[8] состояние брони после отмены: {BookingStatus(booking)}");

        int second = Book(roomId == 101 ? 102 : 101);

        if (second > 0)
        {
            IReadOnlyList<int> opposed = LowerNodeIds(cluster, 2);

            Response split = _client.SendRequest(RequestBuilder.BuildProposeChange(
                "BookingStatus", "Cancelled", second, opposed));
            ConsensusReport? decision = VoteOf(split);

            ClientLog.Line($"[9] голоса разделились: узлы {Join(opposed.Select(id => id.ToString()))} против → " +
                           $"{split.Message.Split('|')[0]} (успех = {split.IsSuccess})");

            if (decision is not null)
            {
                PrintVotes(decision.Votes);
                ClientLog.Line($"[10] YES = {decision.Yes} при большинстве {decision.Majority} → {decision.Decision}, " +
                               $"броня {second}: {BookingStatus(second)}");
            }
        }

        Response version = _client.SendRequest(RequestBuilder.BuildProposeChange(
            "ConfigurationVersion", "15", entityId: 0));

        ClientLog.Line($"[11] значение вне предметной области: {version.Message.Split('|')[0]} " +
                       $"(бронирование не менялось: {BookingStatus(booking)})");

        return true;
    }

    // ---------------------------------------------------------- ЧАСТЬ 6: ЭКСПЕРИМЕНТЫ

    /// <summary>Часть 6: четыре эксперимента с отказами - от полному составу до невозможности консенсуса.</summary>
    public bool RunClusterExperiments(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7 PART 6: FAILURE EXPERIMENTS, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        ResetDatabase();

        var rows = new List<string[]>();
        int[] all = Enumerable.Range(1, Cluster()?.Total ?? 5).ToArray();

        // Четыре эксперимента по заданию: 5/5, 4/5, 3/5 и 2/5 доступных узлов.
        for (int failedCount = 0; failedCount <= all.Length - 2; failedCount++)
        {
            string name = $"Эксперимент {failedCount + 1}: отказано {failedCount} узла(ов)";

            foreach (int node in all.Reverse().Take(failedCount))
                _client.SendRequest(RequestBuilder.BuildFailNode(node, "offline", name));

            Probe();
            ElectionReport? election = Elect(name);

            ResetDatabase();

            int booking = Book(101);
            Response proposal = booking > 0
                ? _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Paid", booking))
                : Response.Error("no booking for the vote");

            ConsensusReport? vote = VoteOf(proposal);
            ClusterStatusReport? state = Cluster();

            string expected = (state?.Available ?? 0) >= (state?.Majority ?? 3)
                ? "CONSENSUS ACHIEVED"
                : "CONSENSUS NOT ACHIEVED";

            string observed = vote is not null && vote.Achieved ? "CONSENSUS ACHIEVED" : "CONSENSUS NOT ACHIEVED";

            rows.Add(new[]
            {
                name,
                $"{state?.Available}/{state?.Total}",
                state?.Majority.ToString() ?? "-",
                election?.LeaderName ?? "-",
                vote?.Yes.ToString() ?? "0",
                vote?.No.ToString() ?? "0",
                vote?.NoResponse.ToString() ?? "0",
                vote?.Refused.ToString() ?? "0",
                observed,
                expected,
                observed == expected ? "да" : "НЕТ"
            });

            ClientLog.Line($"    {name}: доступно = {state?.Available}/{state?.Total}, " +
                           $"большинство = {state?.Majority}, лидер = {election?.LeaderName}, " +
                           $"голосов YES = {vote?.Yes}, решение = {observed}, брони после голосования: " +
                           $"{(booking > 0 ? BookingStatus(booking) : "не создавалась")}");
        }

        PrintTable(ExperimentHeader, rows);

        ClientLog.Line("[1] Эксперимент 1 (5/5): выборы, предложение, голоса и консенсус достигнуты");
        ClientLog.Line("[2] Эксперимент 2 (4/5): повторный выбор лидера, большинство сохраняется, консенсус есть");
        ClientLog.Line("[3] Эксперимент 3 (3/5): большинство ровно 3 из 5 - консенсус ещё возможен");
        ClientLog.Line("[4] Эксперимент 4 (2/5): большинство недостижимо, решение не принимается и не записывается");

        foreach (int node in all)
            _client.SendRequest(RequestBuilder.BuildRestoreNode(node, "nodes are back after the experiments"));

        Elect("nodes are back after the experiments");

        int restoredBooking = Book(roomId == 101 ? 102 : 101);

        if (restoredBooking > 0)
        {
            Response recovered = _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Paid",
                restoredBooking));

            ClientLog.Line($"[5] после возвращения узлов ({Cluster()?.Available}/{Cluster()?.Total}) консенсус " +
                           $"снова достигается: {recovered.Message.Split('|')[0]}");
        }

        return true;
    }

    // ------------------------------------------------------------------ ОБРАБОТКА ОШИБОК

    /// <summary>Требование 2: десять ситуаций ошибок кластера. Ни одна не должна ронять сервер.</summary>
    public bool RunClusterErrors(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR7 PART 4, REQUIREMENT 5: ERROR HANDLING OF THE NODE CLUSTER ===");

        if (!EnsureConsensusMode())
            return false;

        ResetDatabase();

        int booking = Book(roomId);
        int total = Cluster()?.Total ?? 5;
        int before = JournalLines();

        ClientLog.Line("[1] недоступен один узел: Node 1 выключен, сосед замечает это по heartbeat");
        Say("    отказ узла", _client.SendRequest(RequestBuilder.BuildFailNode(1, "offline", "experiment")));
        Say("    проверка доступности", ProbeOrFailure());

        ClientLog.Line("[2] недоступно несколько узлов: к нему добавляются Node 2 (молчит) и Node 4 (выключен)");
        Say("    Node 2 перестал обрабатывать", _client.SendRequest(RequestBuilder.BuildFailNode(2, "stalled", "experiment")));
        Say("    Node 4 выключен", _client.SendRequest(RequestBuilder.BuildFailNode(4, "offline", "experiment")));
        Say("    проверка доступности", ProbeOrFailure());

        ClientLog.Line("[3] отказ текущего лидера: лидер перестаёт отвечать на heartbeat");
        int leaderId = NodeIdOf(Cluster()?.Leader ?? string.Empty) ?? total;

        Say("    отказ лидера", _client.SendRequest(RequestBuilder.BuildFailNode(leaderId, "silent",
            "the leader stopped answering heartbeats")));
        Say("    обнаружение отказа", ProbeOrFailure());
        Say("    повторный выбор лидера", _client.SendRequest(RequestBuilder.BuildStartElection("leader is down")));

        ClientLog.Line("[4] нет ответа на heartbeat: Node 3 отправляет HEARTBEAT узлу, который молчит");
        Say("    HEARTBEAT → Node 2", _client.SendRequest(RequestBuilder.BuildSendNodeMessage(3, 2, "HEARTBEAT")));

        ClientLog.Line("[5] нельзя связаться с узлом: Node 3 отправляет STATUS выключенному Node 1");
        Say("    STATUS → Node 1", _client.SendRequest(RequestBuilder.BuildSendNodeMessage(3, 1, "STATUS")));

        ClientLog.Line("[6] большинства нет: оставляем здоровыми ровно два узла из пяти");

        foreach (int node in Enumerable.Range(1, total))
            _client.SendRequest(RequestBuilder.BuildRestoreNode(node, "restored before the no-majority check"));

        foreach (int node in Enumerable.Range(1, total).Reverse().Skip(2))
            _client.SendRequest(RequestBuilder.BuildFailNode(node, "offline", "no majority experiment"));

        Say("    проверка доступности", ProbeOrFailure());
        Say("    выбор лидера при двух узлах", _client.SendRequest(RequestBuilder.BuildStartElection("only two nodes are left")));

        int lonely = Book(201);

        Say("    попытка голосования", _client.SendRequest(RequestBuilder.BuildProposeChange(
            "BookingStatus", "Paid", lonely)));
        ClientLog.Line($"    броня {lonely} осталась в статусе {BookingStatus(lonely)}: большинства нет, " +
                       "и кластер ничего не записал");

        ClientLog.Line("[7] голос недоступного узла: Node 3 объявлен FAILED, но остальные здоровы и большинство есть");

        foreach (int node in Enumerable.Range(1, total))
            _client.SendRequest(RequestBuilder.BuildRestoreNode(node, "restored before the refused-vote check"));

        Say("    отказ Node 3", _client.SendRequest(RequestBuilder.BuildFailNode(3, "state",
            "the node declares itself FAILED")));
        Say("    выборы без Node 3", _client.SendRequest(RequestBuilder.BuildStartElection("Node 3 is FAILED")));
        Say("    предложение с голосом недоступного узла", _client.SendRequest(RequestBuilder.BuildProposeChange(
            "BookingStatus", "Paid", booking)));
        ClientLog.Line($"    броня {booking}: {BookingStatus(booking)} (отказ одного узла большинства не отменил)");

        ClientLog.Line("[8] повторный запуск выбора лидера: одни и те же выборы подряд");
        Say("    выборы #1", _client.SendRequest(RequestBuilder.BuildStartElection("repeated election 1")));
        Say("    выборы #2", _client.SendRequest(RequestBuilder.BuildStartElection("repeated election 2")));

        ClientLog.Line("[9] несколько сообщений одновременно: параллельная рассылка STATUS всем узлам");
        Say("    broadcast", _client.SendRequest(RequestBuilder.BuildBroadcastNodes("STATUS")));

        var checks = new List<(string Name, Response Response)>
        {
            ("сообщение неизвестного типа",
                _client.SendRequest(RequestBuilder.BuildSendNodeMessage(1, 4, "PING"))),
            ("узел пишет сам себе",
                _client.SendRequest(RequestBuilder.BuildSendNodeMessage(1, 1, "HEARTBEAT"))),
            ("неизвестный узел", _client.SendRequest(RequestBuilder.BuildFailNode(99, "state"))),
            ("неизвестный способ отказа", _client.SendRequest(RequestBuilder.BuildFailNode(2, "earthquake"))),
            ("восстановление неизвестного узла", _client.SendRequest(RequestBuilder.BuildRestoreNode(42))),
            ("нечитаемый payload",
                _client.SendRequest(new Request { Operation = "FailNode", Payload = "{ not json" })),
            ("неизвестное состояние брони",
                _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Shipped", booking))),
            ("BookingStatus без идентификатора брони",
                _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Paid")))
        };

        ClientLog.Line("[10] ошибочные команды и сообщения: сервер отвечает текстом ошибки и остаётся живым");

        foreach ((string name, Response response) in checks)
            Say(name, response);

        Response alive = _client.SendRequest(RequestBuilder.BuildGetServerTime(0));

        ClientLog.Line($"    сервер жив и отвечает после всех ситуаций → IsSuccess = {alive.IsSuccess}");
        ClientLog.Line($"    строк журнала кластера добавилось: {JournalLines() - before}");

        foreach (int node in Enumerable.Range(1, Cluster()?.Total ?? 5))
            _client.SendRequest(RequestBuilder.BuildRestoreNode(node, "restored after error checks"));

        Elect("nodes are back after the error checks");
        ClientLog.Line($"    итог: {Cluster()?.Summary}");

        return true;
    }

    /// <summary>Все части ЛР №7 одним прогоном: используется скриптом экспериментов для итогового журнала.</summary>
    public bool RunClusterFinal(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR7: ALL PARTS IN ONE RUN ===");

        if (!EnsureConsensusMode())
            return false;

        ResetDatabase();

        bool completed = RunClusterNodes(roomId)
                         && RunClusterFailure(roomId)
                         && RunClusterVote(roomId)
                         && RunClusterExperiments(roomId)
                         && RunClusterErrors(roomId);

        ClusterStatisticsReport? counters = Counters();

        if (counters is not null)
        {
            ClientLog.Line($"[LR7] счётчики кластера: {counters.Summary}, сообщений отправлено = " +
                           $"{counters.MessagesSent}, принято = {counters.MessagesAnswered}, голосов = {counters.VotesCast}");
            PrintJournal(counters.Journal);
        }

        return completed;
    }

    // ------------------------------------------------------------------ МЕЛОЧИ СЦЕНАРИЕВ

    /// <summary>
    /// Доказательство для Части 6: пока большинство не собрано, операция клиента не записывается.
    /// Сценарий намеренно оставляет кластер без большинства - состояние таблиц проверяет psql.
    /// </summary>
    public bool RunClusterRefused(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7: NO MAJORITY - NOTHING IS WRITTEN, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        ResetDatabase();
        RestoreAllNodes();

        int total = Cluster()?.Total ?? 5;
        int booking = Book(roomId);

        if (booking == 0)
            return false;

        ClientLog.Line($"[1] броня {booking} создана, её BookingStatus = {BookingStatus(booking)}");

        ClientLog.Line($"[2] оставляем живыми только два узла из {total}: решение принимать нельзя");

        foreach (int node in Enumerable.Range(1, total).Reverse().Skip(2))
            _client.SendRequest(RequestBuilder.BuildFailNode(node, "offline", "no majority proof"));

        Say("    обнаружение отказа", ProbeOrFailure());
        Say("    выбор лидера", _client.SendRequest(RequestBuilder.BuildStartElection("only two nodes are left")));

        Say("    попытка CancelBooking", _client.SendRequest(RequestBuilder.BuildCancelBooking(booking)));
        Say("    попытка PayBooking", _client.SendRequest(RequestBuilder.BuildPayBooking(booking)));

        ClientLog.Line($"[3] BookingStatus брони {booking} после обеих попыток: {BookingStatus(booking)}");
        ClientLog.Line($"[4] кластер остался без большинства: {Cluster()?.Summary}");

        return true;
    }

    /// <summary>Требование 3: два клиента одновременно инициируют предложения - раунды голосования сериализуются.</summary>
    public bool RunClusterClients(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR7: TWO CLIENTS PROPOSE AT THE SAME TIME, room {roomId} ===");

        if (!EnsureConsensusMode())
            return false;

        RestoreAllNodes();

        int booking = Book(roomId);

        if (booking == 0)
            return false;

        Response paid = _client.SendRequest(RequestBuilder.BuildProposeChange("BookingStatus", "Paid", booking));
        ConsensusReport? vote = VoteOf(paid);

        ClientLog.Line($"[1] {ClientLog.Who}: предложение BookingStatus=Paid для брони {booking} → " +
                       $"{paid.Message.Split('|')[0]}");

        if (vote is not null)
        {
            PrintVotes(vote.Votes);
            ClientLog.Line($"[2] голоса: YES = {vote.Yes}, NO = {vote.No}, NO RESPONSE = {vote.NoResponse}, " +
                           $"большинство = {vote.Majority} → {vote.Decision}");
        }

        Response cancelled = _client.SendRequest(RequestBuilder.BuildCancelBooking(booking));
        ClientLog.Line($"[3] вторая операция того же клиента: {cancelled.Message.Split('|')[0]} " +
                       $"(Event = {cancelled.EventTriggered})");
        ClientLog.Line($"[4] BookingStatus в базе: {BookingStatus(booking)}");
        PrintNodes();

        return true;
    }

    private int Book(int roomId)
    {
        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));

        int id = Parse(booking)?.BookingId ?? 0;

        ClientLog.Line($"[LR7] CreateBooking room {roomId} → {(booking.IsSuccess ? "SUCCESS" : "FAIL")}: " +
                       $"{Shorten(booking.Message)} (броня = {id}, Event = {booking.EventTriggered})");

        return id;
    }

    private Response ProbeOrFailure() => _client.SendRequest(RequestBuilder.BuildHeartbeatProbe());

    /// <summary>Одна строка проверки: как в сценариях обработки ошибок ЛР №4-№6.</summary>
    private void Say(string label, Response response) =>
        ClientLog.Line($"    {label,-48} → {(response.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(response.Message)}");

    /// <summary>Идентификатор узла по его имени из отчёта сервера ("Node 4" → 4).</summary>
    private static int? NodeIdOf(string name) =>
        int.TryParse(name.Split(' ').LastOrDefault(), out int id) ? id : null;

    /// <summary>Первая смысловая часть ответа сервера: без хвоста со счётчиками кластера.</summary>
    private static string FirstSegment(string text)
    {
        int cut = text.IndexOf(", available =", StringComparison.OrdinalIgnoreCase);

        return Shorten(cut > 0 ? text[..cut] : text);
    }

    /// <summary>Та часть ответа операции предметной области, где сервер сообщает решение кластера.</summary>
    private static string ConsensusSegment(Response response)
    {
        int at = response.Message.IndexOf("CONSENSUS", StringComparison.OrdinalIgnoreCase);

        return at < 0 ? (response.EventTriggered.Length > 0 ? response.EventTriggered : response.Message)
            : response.Message[at..];
    }

    private int JournalLines() =>
        Read<ClusterJournalReport>(_client.SendRequest(RequestBuilder.BuildClusterJournal(1)))?.Total ?? 0;

    private static IReadOnlyList<int> LowerNodeIds(ClusterStatusReport? cluster, int count) =>
        cluster is null
            ? new List<int> { 1, 2 }
            : cluster.Nodes.Select(node => node.Id).OrderBy(id => id).Take(count).ToList();

    private string BookingStatus(int bookingId) =>
        Read<DatabaseSnapshotReport>(_client.SendRequest(RequestBuilder.BuildDatabaseSnapshot()))?
            .Bookings.FirstOrDefault(booking => booking.Id == bookingId)?.Status ?? "брони нет";
}
