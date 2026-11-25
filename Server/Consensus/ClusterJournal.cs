namespace Server.Consensus;

public sealed class ClusterJournal(int capacity = 400, bool quiet = false)
{
    private static readonly object ConsoleGate = new();
    private readonly object _gate = new();
    private readonly List<JournalEntry> _entries = new();

    public void Add(string who, string marker, string text)
    {
        var entry = new JournalEntry(DateTime.UtcNow.ToString("HH:mm:ss.fff"), who, marker, text);

        lock (_gate)
        {
            _entries.Add(entry);

            if (_entries.Count > capacity)
                _entries.RemoveAt(0);
        }

        if (quiet)
            return;

        lock (ConsoleGate)
            Console.WriteLine(entry.ToString());
    }

    public IReadOnlyList<JournalEntry> Recent(int count)
    {
        lock (_gate)
            return _entries.TakeLast(count).ToList();
    }

    public IReadOnlyList<JournalEntry> Since(int index)
    {
        lock (_gate)
            return _entries.Skip(Math.Max(0, index)).ToList();
    }

    public int Count
    {
        get
        {
            lock (_gate)
                return _entries.Count;
        }
    }

    public void Node(int id, string text) => Add($"Node {id}", "NODE", text);
    public void Heartbeat(int id, string text) => Add($"Node {id}", "HEARTBEAT", text);
    public void Failure(int id, string text) => Add($"Node {id}", "FAILURE", text);
    public void Election(int id, string text) => Add($"Node {id}", "ELECTION", text);
    public void Role(int id, string text) => Add($"Node {id}", "ROLE", text);
    public void Vote(int id, string text) => Add($"Node {id}", "VOTE", text);
    public void Consensus(int id, string text) => Add($"Node {id}", "CONSENSUS", text);
    public void Commit(int id, string text) => Add($"Node {id}", "COMMIT", text);
    public void Domain(int id, string text) => Add($"Node {id}", "DOMAIN", text);
    public void Error(int id, string text) => Add($"Node {id}", "ERROR", text);

    // Строки уровня кластера: их некому приписать, поэтому вместо имени узла - CLUSTER.
    public void Election(string who, string text) => Add(who, "ELECTION", text);
    public void Consensus(string who, string text) => Add(who, "CONSENSUS", text);
    public void Failure(string who, string text) => Add(who, "FAILURE", text);
    public void Error(string who, string text) => Add(who, "ERROR", text);
    public void Cluster(string text) => Add("CLUSTER", "CLUSTER", text);
}
