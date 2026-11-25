namespace Server;


public static class ServerLog
{
    private static readonly object ConsoleGate = new();

    public static void Info(string who, string message)
    {
        lock (ConsoleGate)
        {
            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [{who}] {message}");
        }
    }

    public static void Start(string clientId, string operation) => Info(clientId, $"→ START {operation}");

    public static void Lock(string clientId, string token = "[LOCK]") => Info(clientId, $"→ {token}");

    public static void Unlock(string clientId, string token = "[UNLOCK]") => Info(clientId, $"→ {token}");

    public static void Check(string clientId, string state) => Info(clientId, $"→ CHECK {state}");

    public static void Success(string clientId, string result) => Info(clientId, $"→ SUCCESS {result}");

    public static void Fail(string clientId, string reason) => Info(clientId, $"→ FAIL {reason}");

    public static void Error(string clientId, string message) => Info(clientId, $"→ ERROR {message}");
}
