namespace Client;


public static class ClientLog
{
    private static readonly object ConsoleGate = new();

    public static Func<DateTime>? LocalClock { get; set; }

    public static string Who { get; set; } = "CLIENT";

    public static void Info(string message)
    {
        lock (ConsoleGate)
        {
            DateTime now = LocalClock?.Invoke() ?? DateTime.Now;
            Console.WriteLine($"[{now:HH:mm:ss.fff}] [{Who}] {message}");
        }
    }

    public static void Line(string message)
    {
        lock (ConsoleGate)
        {
            Console.WriteLine(message);
        }
    }
}
