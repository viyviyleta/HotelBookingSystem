using Shared.Configuration;
using Shared.Network;

namespace Client;

internal class Program
{
    private static int Main(string[] args)
    {
        ClientOptions options = ClientOptions.Parse(args);

        // Требование 1 Части 2: часы клиента смоделированы рассинхронизированными.
        var clock = new ClientClock(TimeSpan.FromSeconds(options.ClockOffsetSeconds));
        ClientLog.LocalClock = clock.LocalNow;
        ClientLog.Who = options.Name;

        using var client = new NetworkClient(options.Host, options.Port)
        {
            ClientName = options.Name
        };
        var runner = new ScenarioRunner(client, clock, options.Port, options.Host);

        Console.WriteLine($"[{options.Name}] host={options.Host} port={options.Port} clockSkew={clock.Skew.TotalSeconds:0.###}s");

        if (options.Scenario is null)
            return InteractiveMode(client, runner, options);

        return ScenarioMode(runner, options);
    }

    private static int ScenarioMode(ScenarioRunner runner, ClientOptions options)
    {
        if (!runner.ServerAvailable())
        {
            ClientLog.Info($"ERROR: server {options.Host}:{options.Port} is unavailable. Start the server first.");
            return 2;
        }

        switch (options.Scenario)
        {
            case "reserve":
                runner.ReserveOnce(options.RoomId);
                break;

            case "race":
                runner.RunRace(options.RoomId, options.ClientsCount);
                break;

            case "time":
                runner.PrintLocalTime();
                runner.RunTimeExperiments(options.TimeDelays);
                break;

            case "errors":
                runner.RunErrorChecks();
                break;

            case "db-race":
                if (!runner.EnsureDatabaseMode())
                    return 4;
                runner.RunDatabaseRace(options.RoomId, options.ClientsCount);
                break;

            case "tx":
                if (!runner.EnsureDatabaseMode())
                    return 4;
                runner.RunTransactionExperiments(options.RoomId);
                break;

            case "db-errors":
                if (!runner.EnsureDatabaseMode())
                    return 4;
                runner.RunDatabaseErrorChecks(options.RoomId);
                break;

            case "db-down":
                runner.RunDatabaseOutageChecks(options.RoomId);
                break;

            case "db-final":
                if (!runner.EnsureDatabaseMode())
                    return 4;
                runner.RunDatabaseRace(options.RoomId, options.ClientsCount);
                runner.RunTransactionExperiments(options.RoomId);
                runner.RunDatabaseErrorChecks(options.RoomId);
                break;

            case "repl-status":
                runner.PrintNodeCluster();
                break;

            case "repl":
                if (!runner.RunReplication(options.RoomId))
                    return 4;
                break;

            case "failover":
                if (!runner.RunFailover(options.RoomId))
                    return 4;
                break;

            case "recovery":
                if (!runner.RunRecovery(options.RoomId))
                    return 4;
                break;

            case "repl-cycle":
                if (!runner.RunResilienceCycle(options.RoomId))
                    return 4;
                break;

            case "repl-errors":
                if (!runner.RunReplicationErrors(options.RoomId))
                    return 4;
                break;

            case "repl-final":
                if (!runner.RunReplication(options.RoomId))
                    return 4;
                if (!runner.RunFailover(options.RoomId))
                    return 4;
                if (!runner.RunRecovery(options.RoomId))
                    return 4;
                if (!runner.RunResilienceCycle(options.RoomId))
                    return 4;
                if (!runner.RunReplicationErrors(options.RoomId))
                    return 4;
                break;

            case "pubsub-status":
                runner.PrintPubSubStatus();
                break;

            case "pubsub-basic":
                if (!runner.RunPubSubBasic())
                    return 5;
                break;

            case "pubsub-subscribers":
                if (!runner.RunPubSubSubscribers(options.RoomId))
                    return 5;
                break;

            case "pubsub-failure":
                if (!runner.RunPubSubFailure(options.RoomId))
                    return 5;
                break;

            case "pubsub-new":
                if (!runner.RunPubSubNewSubscriber(options.RoomId))
                    return 5;
                break;

            case "pubsub-cycle":
                if (!runner.RunPubSubCycle(options.RoomId))
                    return 5;
                break;

            case "pubsub-brokers":
                if (!runner.RunPubSubBrokers(options.RoomId))
                    return 5;
                break;

            case "pubsub-errors":
                if (!runner.RunPubSubErrors(options.RoomId))
                    return 5;
                break;

            case "pubsub-final":
                if (!runner.RunPubSubFinal(options.RoomId))
                    return 5;
                break;

            case "cluster-status":
                runner.PrintNodeCluster();
                break;

            case "cluster-nodes":
                if (!runner.RunClusterNodes(options.RoomId))
                    return 6;
                break;

            case "cluster-failure":
                if (!runner.RunClusterFailure(options.RoomId))
                    return 6;
                break;

            case "cluster-vote":
                if (!runner.RunClusterVote(options.RoomId))
                    return 6;
                break;

            case "cluster-experiments":
                if (!runner.RunClusterExperiments(options.RoomId))
                    return 6;
                break;

            case "cluster-errors":
                if (!runner.RunClusterErrors(options.RoomId))
                    return 6;
                break;

            case "cluster-final":
                if (!runner.RunClusterFinal(options.RoomId))
                    return 6;
                break;

            case "cluster-refused":
                if (!runner.RunClusterRefused(options.RoomId))
                    return 6;
                break;

            case "cluster-clients":
                if (!runner.RunClusterClients(options.RoomId))
                    return 6;
                break;

            case "docker-operations":
                if (!runner.RunDockerOperations(options.RoomId))
                    return 8;
                break;

            case "docker-load":
                if (!runner.RunDockerLoad(options.RoomId, options.ClientsCount, options.RequestsPerClient))
                    return 8;
                break;

            case "docker-errors":
                runner.RunDockerErrors(options.RoomId);
                break;

            case "docker-state":
                if (!runner.RunDockerState(options.RoomId))
                    return 8;
                break;

            case "docker-recover":
                if (!runner.RunDockerDatabaseRecovery(options.RoomId))
                    return 8;
                break;

            case "final":
                runner.RunRace(options.RoomId, options.ClientsCount);
                runner.PrintLocalTime();
                runner.RunTimeExperiments(options.TimeDelays);
                runner.RunErrorChecks();
                break;

            default:
                Console.WriteLine($"[CLIENT] Unknown scenario '{options.Scenario}'. Use: reserve | race | time | errors | final " +
                                  "| db-race | tx | db-errors | db-down | db-final " +
                                  "| repl-status | repl | failover | recovery | repl-cycle | repl-errors | repl-final " +
                                  "| pubsub-status | pubsub-basic | pubsub-subscribers | pubsub-failure " +
                                  "| pubsub-new | pubsub-cycle | pubsub-brokers | pubsub-errors | pubsub-final " +
                                  "| cluster-status | cluster-nodes | cluster-failure | cluster-vote " +
                                  "| cluster-experiments | cluster-errors | cluster-final | cluster-refused " +
                                  "| cluster-clients " +
                                  "| docker-operations | docker-load | docker-errors | docker-state | docker-recover");
                return 3;
        }

        return 0;
    }

    private static int InteractiveMode(NetworkClient client, ScenarioRunner runner, ClientOptions options)
    {
        while (true)
        {
            Console.WriteLine("\n=== HOTEL BOOKING SYSTEM ===");
            Console.WriteLine("1. Check Availability");
            Console.WriteLine("2. Create Booking");
            Console.WriteLine("3. Pay Booking");
            Console.WriteLine("4. Cancel Booking");
            Console.WriteLine("5. Send Unknown Operation (Error Test)");
            Console.WriteLine("6. Show my local time (skewed clock)");
            Console.WriteLine("7. Sync time with server (RTT, offset, corrected time)");
            Console.WriteLine("8. Concurrent reserve race (two clients, one room)");
            Console.WriteLine("9. Error handling checks");
            Console.WriteLine("10. Show database state (rooms, bookings, payments)");
            Console.WriteLine("11. Concurrent reserve against the database (two clients, one room)");
            Console.WriteLine("12. Transaction experiments (COMMIT / ROLLBACK / partial change)");
            Console.WriteLine("13. Database error handling checks");
            Console.WriteLine("14. State of both data instances (Primary / Replica)");
            Console.WriteLine("15. Replication experiments (three sequential changes)");
            Console.WriteLine("16. Fail or restore one data instance");
            Console.WriteLine("17. Requests after the Primary failure (Client 1 Read, Client 2 Update)");
            Console.WriteLine("18. Recovery: synchronize the copies and check them");
            Console.WriteLine("19. Replication error handling checks");
            Console.WriteLine("20. Pub/Sub state (Publisher, Brokers, Subscribers)");
            Console.WriteLine("21. Pub/Sub Part 1: basic event delivery");
            Console.WriteLine("22. Pub/Sub Part 2: several subscribers and event types");
            Console.WriteLine("23. Pub/Sub Part 3: async processing and subscriber failure");
            Console.WriteLine("24. Pub/Sub Part 4: new subscriber at runtime");
            Console.WriteLine("25. Pub/Sub Part 4: full event cycle");
            Console.WriteLine("26. Pub/Sub Part 5: two brokers, queue and redelivery");
            Console.WriteLine("27. Pub/Sub error handling checks");
            Console.WriteLine("28. Node cluster state (nodes, roles, leader)");
            Console.WriteLine("29. LR7 Part 1-2: five nodes and leader election");
            Console.WriteLine("30. LR7 Part 3-4: heartbeat, leader failure and re-election");
            Console.WriteLine("31. LR7 Part 5: proposal, voting and majority");
            Console.WriteLine("32. LR7 Part 6: failure experiments (5/5, 4/5, 3/5, 2/5)");
            Console.WriteLine("33. LR7 error handling checks");
            Console.WriteLine("34. LR7: two clients propose at the same time");
            Console.WriteLine("35. LR7: no majority - nothing is written");
            Console.WriteLine("36. LR8: domain operations in the container network");
            Console.WriteLine("37. LR8: load test (concurrent clients, latency statistics)");
            Console.WriteLine("38. LR8: error situations of the containerized application");
            Console.WriteLine("39. LR8: state after a container restart (data, replicas, cluster)");
            Console.WriteLine("40. LR8: recover Primary and Replica after the database container restart");
            Console.WriteLine("0. Exit");
            Console.Write("Select option: ");

            string? choice = Console.ReadLine();
            if (choice == "0" || choice is null) break;   // null — конец ввода (EOF/Ctrl+D)

            try
            {
                switch (choice)
                {
                    case "1":
                    case "2":
                    case "3":
                    case "4":
                    case "5":
                        PrintResponse(client.SendRequest(BuildMenuRequest(choice, options.RoomId)));
                        break;

                    case "6":
                        runner.PrintLocalTime();
                        break;

                    case "7":
                        runner.RunTimeExperiments(new[] { ReadOptionalInt("Server artificial delay, ms [0]: ", 0) });
                        break;

                    case "8":
                        runner.RunRace(options.RoomId, 2);
                        break;

                    case "9":
                        runner.RunErrorChecks();
                        break;

                    case "10":
                        PrintResponse(client.SendRequest(RequestBuilder.BuildDatabaseSnapshot()));
                        break;

                    case "11":
                        if (runner.EnsureDatabaseMode())
                            runner.RunDatabaseRace(options.RoomId, 2);
                        break;

                    case "12":
                        if (runner.EnsureDatabaseMode())
                            runner.RunTransactionExperiments(options.RoomId);
                        break;

                    case "13":
                        if (runner.EnsureDatabaseMode())
                            runner.RunDatabaseErrorChecks(options.RoomId);
                        break;

                    case "14":
                        runner.PrintNodeCluster();
                        break;

                    case "15":
                        runner.RunReplication(options.RoomId);
                        break;

                    case "16":
                        PrintResponse(client.SendRequest(BuildInstanceRequest()));
                        break;

                    case "17":
                        runner.RunFailover(options.RoomId);
                        break;

                    case "18":
                        runner.RunRecovery(options.RoomId);
                        break;

                    case "19":
                        runner.RunReplicationErrors(options.RoomId);
                        break;

                    case "20":
                        runner.PrintPubSubStatus();
                        break;

                    case "21":
                        runner.RunPubSubBasic();
                        break;

                    case "22":
                        runner.RunPubSubSubscribers(options.RoomId);
                        break;

                    case "23":
                        runner.RunPubSubFailure(options.RoomId);
                        break;

                    case "24":
                        runner.RunPubSubNewSubscriber(options.RoomId);
                        break;

                    case "25":
                        runner.RunPubSubCycle(options.RoomId);
                        break;

                    case "26":
                        runner.RunPubSubBrokers(options.RoomId);
                        break;

                    case "27":
                        runner.RunPubSubErrors(options.RoomId);
                        break;

                    case "28":
                        runner.PrintNodeCluster();
                        break;

                    case "29":
                        runner.RunClusterNodes(options.RoomId);
                        break;

                    case "30":
                        runner.RunClusterFailure(options.RoomId);
                        break;

                    case "31":
                        runner.RunClusterVote(options.RoomId);
                        break;

                    case "32":
                        runner.RunClusterExperiments(options.RoomId);
                        break;

                    case "33":
                        runner.RunClusterErrors(options.RoomId);
                        break;

                    case "34":
                        runner.RunClusterClients(options.RoomId);
                        break;

                    case "35":
                        runner.RunClusterRefused(options.RoomId);
                        break;

                    case "36":
                        runner.RunDockerOperations(options.RoomId);
                        break;

                    case "37":
                        runner.RunDockerLoad(options.RoomId, options.ClientsCount, options.RequestsPerClient);
                        break;

                    case "38":
                        runner.RunDockerErrors(options.RoomId);
                        break;

                    case "39":
                        runner.RunDockerState(options.RoomId);
                        break;

                    case "40":
                        runner.RunDockerDatabaseRecovery(options.RoomId);
                        break;

                    default:
                        Console.WriteLine("[CLIENT ERROR] Invalid menu option selected!");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CLIENT ERROR] {ex.Message}");
            }
        }

        return 0;
    }

    /// <summary>Пункт 16: отказ или восстановление одного из экземпляров данных.</summary>
    private static Request BuildInstanceRequest()
    {
        Console.Write("Instance role, primary or replica [primary]: ");
        string role = Normalize(Console.ReadLine(), "primary");

        Console.Write("Action, fail or restore [fail]: ");
        return Normalize(Console.ReadLine(), "fail") == "restore"
            ? RequestBuilder.BuildRestoreInstance(role)
            : RequestBuilder.BuildFailInstance(role);
    }

    private static string Normalize(string? input, string fallback) =>
        string.IsNullOrWhiteSpace(input) ? fallback : input.Trim().ToLowerInvariant();

    private static Request BuildMenuRequest(string choice, int roomId) => choice switch
    {
        "1" => RequestBuilder.BuildCheckAvailability(),
        "2" => RequestBuilder.BuildCreateBooking(),
        "3" => RequestBuilder.BuildPayBooking(),
        "4" => RequestBuilder.BuildCancelBooking(),
        "5" => new Request { Operation = "InvalidOp", Payload = "{}" },
        _ => throw new InvalidOperationException("Invalid menu option selected!")
    };

    private static int ReadOptionalInt(string prompt, int fallback)
    {
        Console.Write(prompt);
        string? input = Console.ReadLine();
        return int.TryParse(input, out int value) && value >= 0 ? value : fallback;
    }

    private static void PrintResponse(Response response)
    {
        Console.WriteLine("\n--- SERVER RESPONSE ---");
        Console.WriteLine($"Success: {response.IsSuccess}");
        Console.WriteLine($"Message: {response.Message}");
        if (!string.IsNullOrEmpty(response.Data))
            Console.WriteLine($"Data: {response.Data}");
        if (!string.IsNullOrEmpty(response.EventTriggered))
            Console.WriteLine($"Event: [{response.EventTriggered}]");
    }
}

internal sealed record ClientOptions(
    string Host,
    int Port,
    string Name,
    double ClockOffsetSeconds,
    string? Scenario,
    int RoomId,
    int ClientsCount,
    int RequestsPerClient,
    int[] TimeDelays)
{
    public static ClientOptions Parse(string[] args)
    {
        // ЛР №8: в контейнере клиент идёт к серверу по имени сервиса, а не к 127.0.0.1.
        string host = Env.Text(Environment.GetEnvironmentVariable("SERVER_HOST"), "127.0.0.1");
        int port = Env.TryNumber(Environment.GetEnvironmentVariable("SERVER_PORT"), 1, 65535, out int envPort)
            ? envPort
            : 5005;
        string name = Env.Text(Environment.GetEnvironmentVariable("CLIENT_NAME"), "Client");
        double offset = 0;
        string? scenario = null;
        int roomId = 101;
        int clients = 2;
        int requests = Env.TryNumber(Environment.GetEnvironmentVariable("LOAD_ROUNDS"), 1, 10000, out int envRounds)
            ? envRounds
            : 20;
        int[] delays = [0, 1000, 3000];

        for (int i = 0; i < args.Length - 1; i++)
        {
            string value = args[i + 1];

            switch (args[i])
            {
                case "--host":
                    host = value;
                    i++;
                    break;
                case "--port":
                    if (int.TryParse(value, out int parsedPort))
                        port = parsedPort;
                    i++;
                    break;
                case "--name":
                    name = value;
                    i++;
                    break;
                case "--offset":
                    if (double.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out double parsedOffset))
                        offset = parsedOffset;
                    i++;
                    break;
                case "--scenario":
                    scenario = value;
                    i++;
                    break;
                case "--room":
                    if (int.TryParse(value, out int parsedRoom))
                        roomId = parsedRoom;
                    i++;
                    break;
                case "--clients":
                    if (int.TryParse(value, out int parsedClients) && parsedClients > 0)
                        clients = parsedClients;
                    i++;
                    break;
                case "--requests":
                    if (int.TryParse(value, out int parsedRequests) && parsedRequests > 0)
                        requests = parsedRequests;
                    i++;
                    break;
                case "--delays":
                    delays = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(int.Parse)
                        .ToArray();
                    i++;
                    break;
                default:
                    Console.WriteLine($"[CLIENT] Ignored unknown argument: {args[i]}");
                    break;
            }
        }

        return new ClientOptions(host, port, name, offset, scenario, roomId, clients, requests, delays.Length > 0 ? delays : [0]);
    }
}
