using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Network;

namespace Client;

/// <summary>
/// Сценарии ЛР №5: две копии данных, передача изменений, отказ Primary, Failover и Recovery.
/// Клиент не знает ничего о СУБД: он только вызывает операции сервера и печатает таблицы по форме задания.
/// </summary>
public sealed partial class ScenarioRunner
{
    private static readonly string[] ReplicationHeader =
        ["№", "Операция", "Primary до", "Primary после", "Replica до", "Replica после", "Репликация", "Результат"];

    private static readonly string[] FailoverHeader =
        ["№", "Клиент", "Операция", "Primary", "Replica", "Результат", "Использованный экземпляр"];

    // ------------------------------------------------------------------ ПРОВЕРКА РЕЖИМА

    public bool EnsureReplicationMode()
    {
        ClusterReport? cluster = Read<ClusterReport>(_client.SendRequest(RequestBuilder.BuildInstanceStatus()));

        if (cluster is null)
        {
            ClientLog.Line("[LR5] the server does not run two data instances.");
            ClientLog.Line("[LR5] start it with: --storage postgres --replication on");
            return false;
        }

        ClientLog.Line($"[LR5] {cluster.Mode}, serving = {cluster.Serving}");

        foreach (InstanceStatus instance in cluster.Instances)
            ClientLog.Line($"[LR5] {instance.Role} = {instance.State.ToUpperInvariant()} " +
                           $"(schema {instance.Schema}, {instance.HealthProbe})");

        return true;
    }

    public void PrintClusterStatus()
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR5: STATE OF BOTH DATA INSTANCES ===");

        if (!EnsureReplicationMode())
            return;

        PrintCopies();
    }

    // ------------------------------------------------- ЧАСТЬ 2: РЕПЛИКАЦИЯ ИЗМЕНЕНИЙ

    public bool RunReplication(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR5 PART 2: REPLICATION OF CHANGES, room {roomId} ===");

        if (!EnsureReplicationMode())
            return false;

        ResetDatabase();

        var rows = new List<string[]>();
        int bookingId = 0;

        // Три последовательных изменения общего ресурса: бронь → оплата → отмена.
        for (int step = 1; step <= 3; step++)
        {
            ConsistencyReport? before = Copies();

            string operation = step switch
            {
                1 => $"CreateBooking room {roomId} (guest 1)",
                2 => $"PayBooking #{bookingId}",
                _ => $"CancelBooking #{bookingId}"
            };

            Response response = _client.SendRequest(step switch
            {
                1 => RequestBuilder.BuildCreateBooking(
                    roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1),
                2 => RequestBuilder.BuildPayBooking(bookingId),
                _ => RequestBuilder.BuildCancelBooking(bookingId)
            });

            ReservationReport? report = Parse(response);
            if (report is not null)
                bookingId = report.BookingId != 0 ? report.BookingId : bookingId;

            ConsistencyReport? after = Copies();

            rows.Add(new[]
            {
                step.ToString(),
                operation,
                State(before?.Primary),
                State(after?.Primary),
                State(before?.Replica),
                State(after?.Replica),
                ReplicationOf(response),
                Verdict(response, after)
            });
        }

        PrintTable(ReplicationHeader, rows);

        Response check = _client.SendRequest(RequestBuilder.BuildCompareReplicas());
        ClientLog.Line($"[LR5] final check: {check.Message}");
        return true;
    }

    // ------------------------------------------------ ЧАСТЬ 3: ОТКАЗ И FAILOVER

    public bool RunFailover(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR5 PART 3: PRIMARY FAILURE AND FAILOVER, room {roomId} ===");

        if (!EnsureReplicationMode())
            return false;

        ResetDatabase();

        Response failure = _client.SendRequest(RequestBuilder.BuildFailInstance("primary"));
        ClientLog.Line($"[LR5] injected failure → {failure.Message}");

        var requests = new (string Client, string Kind, Request Request)[]
        {
            ("Client 1", "Read", RequestBuilder.BuildDatabaseSnapshot()),
            ("Client 2", "Read", RequestBuilder.BuildCheckAvailability()),
            ("Client 1", "Update", RequestBuilder.BuildCreateBooking(
                roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 2)),
            ("Client 2", "Read", RequestBuilder.BuildDatabaseSnapshot())
        };

        var rows = new List<string[]>();
        var details = new List<string>();

        for (int number = 0; number < requests.Length; number++)
        {
            (string who, string kind, Request request) = requests[number];

            ConsistencyReport? copies = Copies();
            Response response = As(who, request);

            rows.Add(new[]
            {
                (number + 1).ToString(),
                who,
                kind + (kind == "Update" ? $" (room {roomId})" : string.Empty),
                Availability(copies, "Primary"),
                Availability(copies, "Replica"),
                response.IsSuccess ? "SUCCESS" : "FAIL",
                UsedInstance(response)
            });

            details.Add($"{number + 1}. {who} {kind} → {Shorten(response.Message)} " +
                        $"(Event = {response.EventTriggered})");
        }

        PrintTable(FailoverHeader, rows);

        ClientLog.Line("--- ответы сервера по каждому запросу ---");

        foreach (string detail in details)
            ClientLog.Line(detail);

        ClientLog.Line($"[LR5] server is still alive (operation without data) → " +
                       $"{As("Client 1", RequestBuilder.BuildGetServerTime(0)).IsSuccess}");
        ClientLog.Line("[LR5] Primary is left UNAVAILABLE on purpose — PART 4 restores it");
        return true;
    }

    // -------------------------------------------------- ЧАСТЬ 4: RECOVERY

    public bool RunRecovery(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR5 PART 4: RECOVERY AND SYNCHRONIZATION, room {roomId} ===");

        if (!EnsureReplicationMode())
            return false;

        Response restored = _client.SendRequest(RequestBuilder.BuildRestoreInstance("primary"));
        ClientLog.Line($"[1] Primary RECOVERY → {restored.Message}");

        ConsistencyReport? diverged = Copies();
        ClientLog.Line($"[2] расхождение после отказа → {diverged?.Summary}");

        foreach (string difference in diverged?.Differences ?? [])
            ClientLog.Line($"[2]   · {difference}");

        ClientLog.Line($"[3] актуальная копия → {UpToDate(diverged)}");

        Response synced = _client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("replica", "primary"));
        ClientLog.Line($"[4] синхронизация Replica → Primary → {synced.Message}");

        ConsistencyReport? matched = Copies();
        ClientLog.Line($"[5] сверка копий → {matched?.Summary}");

        // Контрольная операция: изменение уже согласованных копий через штатный маршрут.
        DatabaseSnapshotReport? state = Read<DatabaseSnapshotReport>(As("Client 1", RequestBuilder.BuildDatabaseSnapshot()));
        BookingRow? active = state?.Bookings.FirstOrDefault(booking => booking.Status != "Cancelled");

        Response control = active is not null
            ? As("Client 1", RequestBuilder.BuildCancelBooking(active.Id))
            : As("Client 1", RequestBuilder.BuildCreateBooking(
                roomId, DateTime.Today.AddDays(5), DateTime.Today.AddDays(7), guestId: 3));

        ClientLog.Line($"[6] контрольная операция после восстановления → " +
                       $"{(control.IsSuccess ? "SUCCESS" : "FAIL")}: {control.Message} " +
                       $"(экземпляр = {UsedInstance(control)}, {ReplicationOf(control)})");

        ConsistencyReport? afterControl = Copies();
        ClientLog.Line($"[7] сверка после контрольной операции → {afterControl?.Summary}");
        return true;
    }

    /// Полный цикл из девяти шагов (Часть 4, Требование 3).
    public bool RunResilienceCycle(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR5: FULL RESILIENCE CYCLE, room {roomId} ===");

        if (!EnsureReplicationMode())
            return false;

        ResetDatabase();
        PrintCopies();
        ClientLog.Line("[1] Primary → AVAILABLE");
        ClientLog.Line("[2] Replica → AVAILABLE");

        Response reserve = As("Client 1", RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        int bookingId = Parse(reserve)?.BookingId ?? 0;
        ClientLog.Line($"[3] Replication → {ReplicationOf(reserve)}");

        ClientLog.Line($"[4] Primary FAILURE → {_client.SendRequest(RequestBuilder.BuildFailInstance("primary")).Message}");

        Response read = As("Client 1", RequestBuilder.BuildDatabaseSnapshot());
        ClientLog.Line($"[5] Failover → запрос обработан через {UsedInstance(read)}");

        // Изменение во время отказа: оплата брони, которую Replica хранит у себя, пока Primary недоступен.
        Response write = As("Client 2", RequestBuilder.BuildPayBooking(bookingId));
        ClientLog.Line($"[6] работа через Replica → {(write.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(write.Message)} " +
                       $"(экземпляр = {UsedInstance(write)})");

        ClientLog.Line($"[7] Primary RECOVERY → {_client.SendRequest(RequestBuilder.BuildRestoreInstance("primary")).Message}");

        ConsistencyReport? diverged = Copies();
        ClientLog.Line($"    после отказа копии различаются → {diverged?.Summary}");

        ClientLog.Line($"[8] Synchronization Replica → Primary → " +
                       $"{_client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("replica", "primary")).Message}");

        ConsistencyReport? final = Copies();
        ClientLog.Line($"[9] Primary = Replica → {final?.Summary}");
        return true;
    }

    // -------------------------------------------- ТРЕБОВАНИЕ 5: ОБРАБОТКА ОШИБОК

    public bool RunReplicationErrors(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR5 PART 4, REQUIREMENT 5: 13 ERROR SITUATIONS ===");

        if (!EnsureReplicationMode())
            return false;

        ResetDatabase();

        // 1..4: недоступность сервера и экземпляров.
        ClientLog.Line($"1.  сервер недоступен            → {OfflineServer().Message}");
        ClientLog.Line($"2.  Primary недоступен           → {_client.SendRequest(RequestBuilder.BuildFailInstance("primary")).Message}");
        ClientLog.Line($"3.  Replica недоступна           → {_client.SendRequest(RequestBuilder.BuildFailInstance("replica")).Message}");
        ClientLog.Line($"4.  оба экземпляра недоступны    → {Shorten(Write(roomId, guestId: 1).Message)}");

        // 5..6: настоящая ошибка соединения моделируется строкой подключения копии (см. scripts/run_lr5_experiments.sh).
        ClientLog.Line("5.  ошибка подключения к Primary → сервер с --primary-conn \"...;Database=hotelbooking-missing\" " +
                       "и --scenario failover (лог 08-primary-down)");
        ClientLog.Line("6.  ошибка подключения к Replica → сервер с --replica-conn той же строки и --scenario repl-status " +
                       "(лог 07-replica-down), копия помечается UNAVAILABLE проверкой соединения");

        // 7: чтение невозможно, когда читать не откуда.
        Response read = _client.SendRequest(RequestBuilder.BuildDatabaseSnapshot());
        ClientLog.Line($"7.  ошибка чтения данных         → {Shorten(read.Message)} (Event = {read.EventTriggered})");

        // 8..10: одиночный отказ копии при живом Primary.
        ClientLog.Line($"    Primary восстановлен         → {_client.SendRequest(RequestBuilder.BuildRestoreInstance("primary")).Message}");
        ClientLog.Line($"8.  ошибка изменения данных      → {Shorten(Write(999, guestId: 1).Message)}");
        Response notReplicated = Write(roomId, guestId: 2);
        ClientLog.Line($"9.  ошибка репликации            → {ReplicationOf(notReplicated)} " +
                       $"(Event = {notReplicated.EventTriggered})");
        ClientLog.Line($"10. нарушение согласованности    → {Shorten(_client.SendRequest(RequestBuilder.BuildCompareReplicas()).Message)}");

        // 11: отказ обоих экземпляров во время запроса → переключаться не на что.
        ClientLog.Line($"    Replica недоступна снова     → {_client.SendRequest(RequestBuilder.BuildFailInstance("replica")).Message}");
        ClientLog.Line($"    Primary недоступен           → {_client.SendRequest(RequestBuilder.BuildFailInstance("primary")).Message}");
        ClientLog.Line($"11. невозможный Failover         → {Shorten(Write(roomId, guestId: 1).Message)}");

        ClientLog.Line($"    Primary восстановлен          → {_client.SendRequest(RequestBuilder.BuildRestoreInstance("primary")).Message}");

        // 12: синхронизация невозможна, когда источник недоступен или когда источник совпадает с приёмником.
        ClientLog.Line($"12. невозможная синхронизация    → {Shorten(_client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("replica", "primary")).Message)}");
        ClientLog.Line($"    источник = приёмник          → {_client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("primary", "primary")).Message}");

        // 13: некорректные запросы клиента.
        ClientLog.Line($"13. некорректный запрос клиента  → {_client.SendRequest(RequestBuilder.BuildInvalidRequest()).Message}");
        ClientLog.Line($"    неизвестная роль экземпляра  → {_client.SendRequest(RequestBuilder.BuildFailInstance("archive")).Message}");
        ClientLog.Line($"    неизвестная операция         → {_client.SendRequest(new Request { Operation = "TeleportRoom", Payload = "{}" }).Message}");

        // Финал: обе копии подняты и синхронизированы, сервер продолжает отвечать.
        ClientLog.Line($"    Replica восстановлена        → {_client.SendRequest(RequestBuilder.BuildRestoreInstance("replica")).Message}");
        ClientLog.Line($"    синхронизация Primary → Replica → " +
                       $"{Shorten(_client.SendRequest(RequestBuilder.BuildSynchronizeReplicas("primary", "replica")).Message)}");
        ClientLog.Line($"    сервер жив после всех ошибок → запрос без БД = {OfflineSafe().IsSuccess}, " +
                       $"чтение = {_client.SendRequest(RequestBuilder.BuildDatabaseSnapshot()).IsSuccess}");

        PrintCopies();
        return true;
    }

    private Response OfflineServer()
    {
        using var closedClient = new NetworkClient("127.0.0.1", 5999);
        return closedClient.SendRequest(RequestBuilder.BuildInstanceStatus());
    }

    private Response OfflineSafe() => _client.SendRequest(RequestBuilder.BuildGetServerTime(0));

    /// <summary>Запись общего ресурса от имени отдельного клиента.</summary>
    private Response Write(int roomId, int guestId) => As($"Client {guestId}",
        RequestBuilder.BuildCreateBooking(roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId));

    // ------------------------------------------------------------------ УТИЛИТЫ

    /// <summary>Снимок обеих копий одной операцией CompareReplicas.</summary>
    private ConsistencyReport? Copies()
    {
        Response response = _client.SendRequest(RequestBuilder.BuildCompareReplicas());
        ConsistencyReport? report = Read<ConsistencyReport>(response);

        if (report is null)
            ClientLog.Line($"[LR5] cannot compare the copies: {response.Message}");

        return report;
    }

    private void PrintCopies()
    {
        ConsistencyReport? report = Copies();

        if (report is null)
            return;

        ClientLog.Line($"[LR5] Primary ({report.Primary.Schema}): {State(report.Primary)}, instance state = " +
                       $"{(report.Primary.IsAvailable ? "AVAILABLE" : "UNAVAILABLE")}");
        ClientLog.Line($"[LR5] Replica ({report.Replica.Schema}): {State(report.Replica)}, instance state = " +
                       $"{(report.Replica.IsAvailable ? "AVAILABLE" : "UNAVAILABLE")}");
        ClientLog.Line($"[LR5] {report.Summary}");
    }

    /// Состояние копии одной строкой: сколько свободных номеров, броней и платежей.
    private static string State(CopySnapshot? copy) => copy is null
        ? "no data"
        : $"free {copy.AvailableRooms}/{copy.Rooms}, " +
          $"bookings {copy.Bookings} (active {copy.ActiveBookings}), payments {copy.Payments}";

    private static string Availability(ConsistencyReport? copies, string role)
    {
        CopySnapshot? copy = role == "Primary" ? copies?.Primary : copies?.Replica;
        string state = copy?.IsAvailable == true ? "AVAILABLE" : "UNAVAILABLE";
        return $"{state} / {State(copy)}";
    }

    private static string ReplicationOf(Response response)
    {
        string text = Field(response, "Replication");
        return text.Length == 0 ? "not used (read or rejected change)" : Shorten(text);
    }

    private static string UsedInstance(Response response)
    {
        string served = Field(response, "ServedBy");
        if (served.Length > 0)
            return served;

        Match match = Regex.Match(response.Message, @"read from (?<name>\w+) =");
        return match.Success ? match.Groups["name"].Value : "unknown";
    }

    /// <summary>Поле из Data ответа сервера, независимо от формы отчёта операции.</summary>
    private static string Field(Response response, string name)
    {
        if (string.IsNullOrEmpty(response.Data))
            return string.Empty;

        try
        {
            using JsonDocument document = JsonDocument.Parse(response.Data);

            if (document.RootElement.ValueKind != JsonValueKind.Object)
                return string.Empty;

            foreach (JsonProperty property in document.RootElement.EnumerateObject())
                if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                    return property.Value.ValueKind == JsonValueKind.String
                        ? property.Value.GetString() ?? string.Empty
                        : property.Value.ToString();
        }
        catch (JsonException)
        {
            // Data не является объектом JSON (например, список номеров) — поля нет.
        }

        return string.Empty;
    }

    private static string Verdict(Response response, ConsistencyReport? copies)
    {
        string result = response.IsSuccess ? "SUCCESS" : "FAIL";

        if (copies is not null)
            result += copies.IsConsistent ? ", copies match" : ", DIVERGED";

        return result;
    }

    private static string UpToDate(ConsistencyReport? report)
    {
        if (report is null)
            return "unknown";

        if (report.IsConsistent)
            return "copies are equal";

        return report.Replica.Bookings + report.Replica.Payments >
               report.Primary.Bookings + report.Primary.Payments
            ? "Replica (изменения периода отказа применены только в ней)"
            : "Primary (Replica отстаёт: репликация не прошла)";
    }

    private Response As(string name, Request request)
    {
        using var client = new NetworkClient(_host, _port)
        {
            ClientName = name
        };

        return client.SendRequest(request);
    }

    private static T? Read<T>(Response response) where T : class =>
        string.IsNullOrEmpty(response.Data)
            ? null
            : JsonSerializer.Deserialize<T>(response.Data, Json);

    private static string Shorten(string text) =>
        text.Length <= 120 ? text : text[..117] + "...";

    private static void PrintTable(string[] header, IReadOnlyList<string[]> rows)
    {
        int[] width = header
            .Select((_, column) => new[] { header[column] }.Concat(rows.Select(row => row[column]))
                .Max(cell => cell.Length))
            .ToArray();

        string Rule() => "+" + string.Join("+", width.Select(size => new string('-', size + 2))) + "+";
        string Row(string[] cells) => "| " + string.Join(" | ", cells
            .Select((cell, column) => cell.PadRight(width[column]))) + " |";

        ClientLog.Line(Rule());
        ClientLog.Line(Row(header));
        ClientLog.Line(Rule());

        foreach (string[] row in rows)
            ClientLog.Line(Row(row));

        ClientLog.Line(Rule());
    }
}
