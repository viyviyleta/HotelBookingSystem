using Server;

namespace ServerSemaphore;

internal class Program
{
    private static async Task Main(string[] args)
    {

        CommandLine.Apply(args.Length == 0 ? ["--port", "5006"] : args);
        ServerConfig.SyncMode = "SemaphoreSlim";

        var listener = new ServerListener(ServerConfig.Port, new RequestHandler(new SemaphoreSlimBusinessLogic()));

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        await listener.StartAsync(cts.Token);
    }
}
