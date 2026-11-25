using System.Collections.Concurrent;
using System.Diagnostics;
using Shared.Network;

namespace Client;

/// <summary>
/// ЛР №8: сценарии проверки приложения, запущенного в контейнерах Docker. Сам клиент -
/// тоже контейнер в пользовательской сети: он обращается к серверу по имени сервиса
/// (SERVER_HOST=server), а не к localhost, поэтому код сценариев не меняется,
/// меняется только окружение. Добавлены проверка операций предметной области в
/// контейнерной сети, нагрузочное тестирование и ошибки контейнеризованного запуска.
/// </summary>
public sealed partial class ScenarioRunner
{
    private static readonly int[] LoadRooms = [101, 102, 201];

    private static readonly string[] LoadHeader =
        ["Фаза", "Запросов", "Успешно", "Ошибок", "Стена, мс", "Запросов/с", "Мин, мс", "Среднее, мс", "p50, мс", "p95, мс", "Макс, мс"];

    // --------------------------------------- Часть 3: операции предметной области

    public bool RunDockerOperations(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR8 PART 3: DOMAIN OPERATIONS IN THE CONTAINER NETWORK ===");
        ClientLog.Line($"Target: {_host}:{_port} - in Docker Compose this is the service name, not localhost");

        if (!EnsureDatabaseMode())
            return false;

        ResetDatabase();
        PrintRoomState("before", roomId);

        Say("1. CheckAvailability (read)", _client.SendRequest(RequestBuilder.BuildCheckAvailability()));

        Response created = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        int bookingId = Parse(created)?.BookingId ?? 0;

        Say("2. CreateBooking (INSERT bookings + UPDATE rooms)", created);
        PrintRoomState("after booking", roomId);

        Say("3. PayBooking (INSERT payments + UPDATE bookings)",
            _client.SendRequest(RequestBuilder.BuildPayBooking(bookingId)));

        Say("4. CancelBooking (free the room)",
            _client.SendRequest(RequestBuilder.BuildCancelBooking(bookingId)));
        PrintRoomState("after cancel", roomId);

        Say("5. CheckAvailability again (read)", _client.SendRequest(RequestBuilder.BuildCheckAvailability()));

        PrintClusterStatus();
        PrintPubSubStatus();
        PrintNodeCluster();

        return true;
    }

    // ------------------------------------------- Часть 4: состояние после перезапуска

    public bool RunDockerState(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR8 PART 4: STATE OF THE CONTAINERIZED APPLICATION ===");

        if (!EnsureDatabaseMode())
            return false;

        PrintRoomState("current", roomId);

        Response snapshot = _client.SendRequest(RequestBuilder.BuildDatabaseSnapshot());
        ClientLog.Line($"[DB] snapshot → {Shorten(snapshot.Message)}");

        Response copies = _client.SendRequest(RequestBuilder.BuildCompareReplicas());
        ClientLog.Line($"[LR8] Primary и Replica → {Shorten(copies.Message)}");

        PrintNodeCluster();
        PrintPubSubStatus();

        return true;
    }

    // ---------------------------------- Часть 4: восстановление после остановки БД

    public bool RunDockerDatabaseRecovery(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR8 PART 4: DATABASE RECOVERY AFTER THE CONTAINER RESTART ===");

        Response primary = _client.SendRequest(RequestBuilder.BuildRestoreInstance("primary"));
        ClientLog.Line($"[1] Primary восстановлен  → {(primary.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(primary.Message)}");

        Response replica = _client.SendRequest(RequestBuilder.BuildRestoreInstance("replica"));
        ClientLog.Line($"[2] Replica восстановлена → {(replica.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(replica.Message)}");

        Response synced = _client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("primary", "replica"));
        ClientLog.Line($"[3] Синхронизация Primary → Replica → {(synced.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(synced.Message)}");

        Response copies = _client.SendRequest(RequestBuilder.BuildCompareReplicas());
        ClientLog.Line($"[4] Сверка копий → {Shorten(copies.Message)}");

        PrintRoomState("after recovery", roomId);
        PrintNodeCluster();

        return primary.IsSuccess && replica.IsSuccess;
    }

    // ------------------------------------------- Часть 4: нагрузочное тестирование

    public bool RunDockerLoad(int roomId, int clients, int rounds)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR8 PART 4: LOAD TEST, {clients} concurrent clients x {rounds} rounds ===");

        if (!EnsureDatabaseMode())
            return false;

        ResetDatabase();

        var rows = new List<string[]>
        {
            LoadPhase("Чтение: CheckAvailability", clients, rounds,
                (connection, worker, round) => [connection.SendTimed(RequestBuilder.BuildCheckAvailability())]),

            LoadPhase("Чтение: DatabaseSnapshot", clients, rounds,
                (connection, worker, round) => [connection.SendTimed(RequestBuilder.BuildDatabaseSnapshot())]),

            LoadPhase("Запись: цикл брони (Create, Pay, Cancel)", clients, rounds,
                (connection, worker, round) => BookingCycle(connection, LoadRooms[(worker - 1) % LoadRooms.Length], worker, round))
        };

        // Спор за один номер начинается с чистого состояния: предыдущая фаза могла оставить
        // номер занятым, и тогда все восемь клиентов получили бы отказ ещё до начала спора.
        ResetDatabase();

        rows.Add(LoadPhase($"Спор {clients} клиентов за номер {roomId}", clients, 1,
            (connection, worker, round) => [connection.SendTimed(RequestBuilder.BuildCreateBooking(
                roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1 + (worker - 1) % 3))]));

        PrintTable(LoadHeader, rows);

        ResetDatabase();
        PrintRoomState("after load", roomId);

        Response copies = _client.SendRequest(RequestBuilder.BuildCompareReplicas());
        ClientLog.Line($"[LR8] Primary и Replica после нагрузки → {copies.Message}");

        return true;
    }

    private string[] LoadPhase(string label, int clients, int rounds,
        Func<NetworkClient, int, int, TimedResponse[]> work)
    {
        var latencies = new ConcurrentQueue<double>();
        int[] succeeded = new int[clients];
        int[] rejected = new int[clients];

        using var gate = new ManualResetEventSlim(false);
        Stopwatch wall = Stopwatch.StartNew();

        Task[] workers = Enumerable.Range(1, clients).Select(worker => Task.Run(() =>
        {
            using var connection = new NetworkClient(_host, _port) { ClientName = $"load-{worker}" };

            // Прогрев соединения: время установки TCP не должно попадать в статистику.
            connection.SendRequest(RequestBuilder.BuildGetServerTime(0));

            gate.Wait();

            for (int round = 1; round <= rounds; round++)
            {
                foreach (TimedResponse timed in work(connection, worker, round))
                {
                    latencies.Enqueue((timed.ReceivedAt - timed.SentAt).TotalMilliseconds);

                    if (timed.Response.IsSuccess)
                        succeeded[worker - 1]++;
                    else
                        rejected[worker - 1]++;
                }
            }
        })).ToArray();

        gate.Set();
        Task.WhenAll(workers).GetAwaiter().GetResult();
        wall.Stop();

        List<double> sorted = latencies.OrderBy(value => value).ToList();
        int total = sorted.Count;
        int ok = succeeded.Sum();
        int fail = rejected.Sum();
        double seconds = wall.Elapsed.TotalSeconds;
        double rate = seconds <= 0 ? 0 : total / seconds;

        ClientLog.Line($"[LOAD] {label}: {total} запросов за {wall.ElapsedMilliseconds} мс, " +
                       $"{rate.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture)} запросов/с, " +
                       $"успешно {ok}, отказов {fail}");

        return
        [
            label,
            total.ToString(),
            ok.ToString(),
            fail.ToString(),
            wall.ElapsedMilliseconds.ToString(),
            rate.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture),
            Ms(sorted.FirstOrDefault()),
            Ms(total == 0 ? 0 : sorted.Average()),
            Ms(Percentile(sorted, 0.50)),
            Ms(Percentile(sorted, 0.95)),
            Ms(sorted.LastOrDefault())
        ];
    }

    /// <summary>Один круг записи: забронировать, оплатить, отменить. Номер у каждого клиента свой,
    /// поэтому отмена освобождает его и следующий круг снова проходит целиком.</summary>
    private static TimedResponse[] BookingCycle(NetworkClient connection, int roomId, int worker, int round)
    {
        TimedResponse created = connection.SendTimed(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1 + (worker + round) % 3));

        int bookingId = Parse(created.Response)?.BookingId ?? 0;

        if (bookingId == 0)
            return [created];

        TimedResponse paid = connection.SendTimed(RequestBuilder.BuildPayBooking(bookingId));
        TimedResponse cancelled = connection.SendTimed(RequestBuilder.BuildCancelBooking(bookingId));

        return [created, paid, cancelled];
    }

    private static double Percentile(IReadOnlyList<double> sorted, double level) =>
        sorted.Count == 0
            ? 0
            : sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(sorted.Count * level) - 1)];

    private static string Ms(double value) =>
        value.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture);

    // ------------------------------------ Часть 4: ошибки контейнеризованного запуска

    public void RunDockerErrors(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR8 PART 4: ERROR SITUATIONS ===");

        ResetDatabase();

        Say("Неизвестная операция",
            _client.SendRequest(new Request { Operation = "TeleportRoom", Payload = "{}" }));

        Say("Некорректный JSON в payload",
            _client.SendRequest(RequestBuilder.BuildInvalidRequest()));

        Say("Пустой payload",
            _client.SendRequest(new Request { Operation = "CreateBooking", Payload = "" }));

        Say("Несуществующий номер",
            _client.SendRequest(RequestBuilder.BuildCreateBooking(999, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), 1)));

        Say("Несуществующая бронь",
            _client.SendRequest(RequestBuilder.BuildPayBooking(4242)));

        Say("Выезд раньше заселения",
            _client.SendRequest(new Request
            {
                Operation = "CreateBooking",
                Payload = """{"RoomId":101,"GuestId":1,"CheckInDate":"2030-05-10","CheckOutDate":"2030-05-01"}"""
            }));

        Say("Несуществующий гость (внешний ключ)",
            _client.SendRequest(RequestBuilder.BuildCreateBooking(102, DateTime.Today.AddDays(1), DateTime.Today.AddDays(2), guestId: 99999)));

        Say("Сервер недоступен (порт без контейнера)",
            new NetworkClient(_host, 5999).SendRequest(RequestBuilder.BuildCheckAvailability()));

        Say("Состояние базы данных",
            _client.SendRequest(RequestBuilder.BuildDatabaseHealth()));

        Response created = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), 1));
        int bookingId = Parse(created)?.BookingId ?? 0;

        Say("CreateBooking при текущем состоянии базы", created);

        Say("Сервер жив после отказа базы (операция без БД)",
            _client.SendRequest(RequestBuilder.BuildGetServerTime(0)));

        if (bookingId > 0)
        {
            Say("Оплата брони", _client.SendRequest(RequestBuilder.BuildPayBooking(bookingId)));

            Say("Оплата уже оплаченной брони",
                _client.SendRequest(RequestBuilder.BuildPayBooking(bookingId)));

            Say("Отмена брони", _client.SendRequest(RequestBuilder.BuildCancelBooking(bookingId)));

            Say("Отмена уже отменённой брони",
                _client.SendRequest(RequestBuilder.BuildCancelBooking(bookingId)));
        }

        int busyRoom = LoadRooms.First(candidate => candidate != roomId);

        _client.SendRequest(RequestBuilder.BuildCreateBooking(
            busyRoom, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 2));

        Say("Повторная бронь занятого номера",
            _client.SendRequest(RequestBuilder.BuildCreateBooking(
                busyRoom, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 3)));

        PrintRoomState("final", roomId);
    }
}
