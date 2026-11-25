using Server.Consensus;

namespace Tests;

/// <summary>
/// Общие инструменты тестов ЛР №7. Узлы поднимаются на свободных портах, которые выдаёт ОС: в прогоне
/// тестов пять фиксированных портов могут быть заняты, а проверяется при этом одно и то же.
/// Журнал в тестах работает в Quiet-режиме, чтобы прогон не выводил тысячи строк.
/// </summary>
internal static class TestCluster7
{
    /// <summary>Таймаут heartbeat в тестах: узел, который молчит, должен быть замечен быстро.</summary>
    public const int TimeoutMs = 400;

    public static ClusterJournal Quiet() => new(capacity: 200, quiet: true);

    /// <summary>Один узел без сокетов: проверка его состояния, роли и реакции на сообщения.</summary>
    public static ClusterNode Node(int id) => new(id, Quiet());

    public static ClusterMessage Message(string type, int from, int to, string value = "",
        IReadOnlyList<int>? oppose = null, string proposal = "P1", string subject = "BookingStatus") =>
        new()
        {
            Type = type,
            From = from,
            To = to,
            Value = value,
            Subject = subject,
            Oppose = oppose ?? [],
            Proposal = proposal
        };

    /// <summary>
    /// Кластер из N узлов с уже выбранным лидером: тест получает готовую топологию и сам моделирует
    /// отказы, выборы и голосования.
    /// </summary>
    public static ClusterManager Cluster(int nodes = 5, int timeoutMs = TimeoutMs)
    {
        var journal = new ClusterJournal(capacity: 400, quiet: true);
        var cluster = new ClusterManager(
            Enumerable.Range(1, nodes).Select(id => new ClusterNode(id, journal)).ToList(),
            journal,
            timeoutMs);

        cluster.StartOnFreePorts("Tester");
        cluster.Elect("test start", "Tester");

        return cluster;
    }

    /// <summary>Лидер кластера по состоянию узлов, а не по памяти теста.</summary>
    public static ClusterNode? Leader(ClusterManager cluster) =>
        cluster.Nodes.FirstOrDefault(node => node.Role == NodeRole.Leader);

    /// <summary>Голос предложения, которое лидер разослал всем: список "узел → итог".</summary>
    public static Dictionary<string, string> VoteByNode(ConsensusReport report) =>
        report.Votes.ToDictionary(vote => vote.Node, vote => vote.Result);
}
