using Shared.Network;

namespace Client;

/// <summary>
/// Сценарии ЛР №6: Publisher, Broker, Subscriber. Клиент инициирует операции предметной области,
/// вручную публикует события и управляет подписками, а затем печатает таблицы по форме задания:
/// подписки, доставка событий, времена обработки и сравнение двух способов взаимодействия.
/// </summary>
public sealed partial class ScenarioRunner
{
    private static readonly string[] SubscriptionHeader =
        ["Broker", "Subscriber", "Назначение", "Подписки", "Состояние", "Задержка, мс", "Обработано", "Ошибок", "В очереди"];

    private static readonly string[] DeliveryHeader =
        ["№", "Клиент", "Событие", "Тип события", "Broker", "Subscriber", "Результат", "Попыток", "Обработка, мс", "Комментарий"];

    private static readonly string[] TimingHeader =
        ["Событие", "Клиент", "Время публикации, мс", "Время доставки, мс", "Время обработки, мс", "Завершение всех Subscriber, мс"];

    private static readonly string[] CouplingHeader = ["Критерий", "Прямое взаимодействие", "Pub/Sub"];

    // ------------------------------------------------------------------ ПРОВЕРКА РЕЖИМА

    /// <summary>Механизм событий поднимается только флагом --pubsub on: без него это понятный отказ, а не ошибка клиента.</summary>
    public bool EnsurePubSubMode()
    {
        PubSubStatusReport? report = Status();

        if (report is null)
        {
            ClientLog.Line("[LR6] the server runs without the Pub/Sub mechanism.");
            ClientLog.Line("[LR6] start it with: --storage postgres --replication on --pubsub on");
            return false;
        }

        ClientLog.Line($"[LR6] {report.Summary}");
        return true;
    }

    public void PrintPubSubStatus()
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR6: PUBLISHER, BROKERS AND SUBSCRIBERS ===");

        if (!EnsurePubSubMode())
            return;

        PrintSubscriptions();
    }

    // ------------------------------------------- ЧАСТЬ 1: БАЗОВАЯ МОДЕЛЬ PUB/SUB

    /// <summary>Часть 1, Требование 6: Publisher → Broker → Subscriber, событие дошло до подписчика.</summary>
    public bool RunPubSubBasic()
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR6 PART 1: BASIC PUB/SUB (Publisher → Broker → Subscriber) ===");

        if (!EnsurePubSubMode())
            return false;

        ResetPubSub();
        PrintSubscriptions();

        int seen = EventCount();

        Response publish = _client.SendRequest(RequestBuilder.BuildPublishEvent(
            "BookingCreated", entityId: 101, roomId: "101",
            data: new Dictionary<string, string> { ["guest"] = "1", ["total"] = "200.00" }));

        ClientLog.Line("[1] Client → PublishEvent BookingCreated (entity 101)");
        ClientLog.Line($"[2] Server → Event, Publisher → Broker, Broker → Subscriber → {publish.Message}");
        ClientLog.Line($"[3] ответ сервера: IsSuccess = {publish.IsSuccess}, EventTriggered = {publish.EventTriggered}");

        foreach (EventTiming entry in NewEvents(seen))
        {
            ClientLog.Line($"[4] {entry.EventType} #{entry.EventId}: публикация {entry.PublishMs} мс, " +
                           $"все подписчики завершились через {entry.AllDoneMs} мс");

            foreach (SubscriberTiming subscriber in entry.Subscribers)
                ClientLog.Line($"[5] Broker {subscriber.Broker} → {subscriber.Subscriber} → {subscriber.Outcome} " +
                               $"({subscriber.ProcessingMs} мс): {subscriber.Result}");

            ClientLog.Line($"[6] событие получил(а): {Join(Names(entry))}, " +
                           $"Broker принял = {entry.Publication}");
        }

        return true;
    }

    // --------------------------------- ЧАСТЬ 2: НЕСКОЛЬКО ПОДПИСЧИКОВ И ТИПОВ СОБЫТИЙ

    /// <summary>Три реальные операции предметной области дают три типа событий и три разных набора подписчиков.</summary>
    public bool RunPubSubSubscribers(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR6 PART 2: SEVERAL SUBSCRIBERS AND EVENT TYPES, room {roomId} ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();
        PrintSubscriptions();

        int seen = EventCount();

        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;
        Report(booking);

        Response payment = _client.SendRequest(RequestBuilder.BuildPayBooking(BookingId));
        Report(payment);

        Response cancel = _client.SendRequest(RequestBuilder.BuildCancelBooking(BookingId));
        Report(cancel);

        List<EventTiming> events = NewEvents(seen);

        ClientLog.Line("--- журнал доставки: событие → подписчики ---");
        PrintTable(DeliveryHeader, DeliveryOf(events));

        ClientLog.Line("--- Subscriber получает только интересующие его события ---");

        foreach (EventTiming entry in events)
            ClientLog.Line($"[LR6] {entry.EventType} #{entry.EventId}: получили = {Join(Names(entry))}, " +
                           $"не получили = {Join(AllSubscriberNames().Except(Names(entry)))}");

        ClientLog.Line($"[LR6] типов событий в эксперименте: {Join(events.Select(entry => entry.EventType).Distinct())} " +
                       $"(по заданию нужно не менее трёх)");

        return true;
    }

    // --------------------- ЧАСТЬ 3: АСИНХРОННАЯ ОБРАБОТКА И ОТКАЗ SUBSCRIBER

    public bool RunPubSubFailure(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR6 PART 3: ASYNCHRONOUS PROCESSING AND SUBSCRIBER FAILURE, room {roomId} ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();

        int seen = EventCount();

        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;

        ClientLog.Line($"[1] сервер ответил клиенту, не дожидаясь подписчиков: {Shorten(booking.Message)}");
        PrintTimings(NewEvents(seen));

        ClientLog.Line("[2] искусственные задержки подписчиков заданы конфигурацией сервера (--event-delay):");
        foreach (SubscriberStatus subscriber in Subscribers())
            ClientLog.Line($"      {subscriber.Name,-20} → {subscriber.DelayMs} мс, " +
                           $"обработано = {subscriber.Handled}, ошибок = {subscriber.Failed}");

        ClientLog.Line($"[3] остановка подписчика (изменение состояния) → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "RoomService", "unavailable", reason: "subscriber stopped by the experiment")).Message)}");

        seen = EventCount();
        Response created = _client.SendRequest(RequestBuilder.BuildPublishEvent("BookingCreated", BookingId,
            roomId: roomId.ToString(), data: new Dictionary<string, string> { ["guest"] = "1", ["total"] = "200.00" }));
        ClientLog.Line($"[4] новое событие после отказа → {Shorten(created.Message)} (Event = {created.EventTriggered})");

        foreach (EventTiming entry in NewEvents(seen))
        {
            ClientLog.Line($"[5] {entry.EventType} #{entry.EventId}: начало и завершения обработки каждого подписчика");

            foreach (SubscriberTiming subscriber in entry.Subscribers)
                ClientLog.Line($"      {subscriber.Broker} → {subscriber.Subscriber}: старт {subscriber.StartedAt}, " +
                               $"финиш {subscriber.FinishedAt}, {subscriber.ProcessingMs} мс → {subscriber.Outcome}" +
                               (string.IsNullOrEmpty(subscriber.Error) ? string.Empty : $" ({subscriber.Error})"));

            ClientLog.Line($"[6] отказ зафиксирован: успешно = {Count(entry, "SUCCESS")}, недоступно = " +
                           $"{Count(entry, "UNAVAILABLE")}, в очереди Broker = " +
                           $"{entry.Subscribers.Count(subscriber => subscriber.Outcome != "SUCCESS")}");
        }

        ClientLog.Line($"[7] остальные подписчики работают, сервер жив: {OfflineSafe().IsSuccess}; состояние → " +
                       $"{Shorten(_client.SendRequest(RequestBuilder.BuildPubSubStatus()).Message)}");

        ClientLog.Line($"[8] подписчик восстановлен → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreSubscriber("RoomService")).Message)}");

        seen = EventCount();
        ClientLog.Line($"[9] повторная доставка из очереди → {Shorten(_client.SendRequest(RequestBuilder.BuildRetryPending()).Message)}");
        PrintTimings(NewEvents(seen));

        ClientLog.Line("[10] другие способы отказа из задания: ошибка соединения и искусственное исключение");
        ClientLog.Line($"      ошибка соединения с Logger → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "Logger", "connection", reason: "connection to the subscriber was lost")).Message)}");
        ClientLog.Line($"      ошибка обработки у NotificationService → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "NotificationService", "processing", reason: "handler threw an exception")).Message)}");

        seen = EventCount();
        _client.SendRequest(RequestBuilder.BuildPublishEvent("PaymentCompleted", BookingId,
            data: new Dictionary<string, string> { ["amount"] = "200.00" }));

        foreach (EventTiming entry in NewEvents(seen))
            ClientLog.Line($"[11] {entry.EventType} #{entry.EventId}: {Join(entry.Subscribers.Select(subscriber =>
                $"{subscriber.Subscriber} = {subscriber.Outcome}"))} → Broker обработал ошибку доставки, " +
                           $"события остались в его очереди");

        ClientLog.Line($"[12] подписчики подняты → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreSubscriber("Logger")).Message)} | {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreSubscriber("NotificationService")).Message)}");
        ClientLog.Line($"[13] повторная доставка всех очередей → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRetryPending()).Message)}");
        ClientLog.Line($"[14] Broker больше не хранит событий: в очереди = {Status()?.Brokers.Sum(broker => broker.Pending) ?? 0}");
        return true;
    }

    // ------------------------------------------- ЧАСТЬ 4: НОВЫЙ SUBSCRIBER И СРАВНЕНИЕ

    /// <summary>Требование 1: подписчик появляется во время работы процесса, код Publisher не меняется.</summary>
    public bool RunPubSubNewSubscriber(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR6 PART 4: NEW SUBSCRIBER AT RUNTIME, room {roomId} ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();

        ClientLog.Line($"[1] приложение запущено; Publisher знает только Broker-ы: {Join(Brokers().Select(broker => broker.Name))}");

        int before = Subscribers().Count;

        Response added = _client.SendRequest(RequestBuilder.BuildAddSubscriber(
            "AuditService", "audit", "broker-1", new[] { "BookingCreated", "BookingCancelled" }));
        ClientLog.Line($"[2] новый Subscriber зарегистрирован Broker-ом → {Shorten(added.Message)}");

        Response subscribed = _client.SendRequest(RequestBuilder.BuildSubscribeEvent("AuditService", "PaymentCompleted"));
        ClientLog.Line($"[3] подписка на существующее событие → {Shorten(subscribed.Message)}");

        int seen = EventCount();
        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;
        ClientLog.Line($"[4] операция предметной области → {Shorten(booking.Message)} (Event = {booking.EventTriggered})");

        foreach (EventTiming entry in NewEvents(seen))
        {
            SubscriberTiming? audit = entry.Subscribers.FirstOrDefault(s => s.Subscriber == "AuditService");
            ClientLog.Line($"[5] {entry.EventType} #{entry.EventId} получили: {Join(Names(entry))}");
            ClientLog.Line($"[6] новый Subscriber обработал событие → {(audit is null ? "NO" : "YES")}: " +
                           $"{audit?.Result ?? "нет доставки"}");
        }

        ClientLog.Line($"[7] журнал: подписчиков было {before}, стало {Subscribers().Count}; " +
                       $"{Status()?.Summary}");
        PrintSubscriptions();

        ClientLog.Line("");
        ClientLog.Line("=== LR6 PART 4: DIRECT INTERACTION VERSUS PUB/SUB ===");
        PrintTable(CouplingHeader, CouplingRows());

        ClientLog.Line("[LR6] Publisher не пришлось менять: в EventPublisher есть только список EventBroker " +
                       "и ни одного обращения к ISubscriber (это проверяет тест).");
        return true;
    }

    /// <summary>Требование 3: полный цикл из десяти шагов.</summary>
    public bool RunPubSubCycle(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR6 PART 4: FULL PUB/SUB CYCLE, room {roomId} ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();
        PrintSubscriptions();

        int start = EventCount();
        int seen = start;

        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;
        EventTiming created = LastOf(NewEvents(seen));

        ClientLog.Line($"[1] сервер выполнил операцию CreateBooking → {Shorten(booking.Message)}");
        ClientLog.Line($"[2] сформировано событие {created.EventType} #{created.EventId} (entity = {created.EntityId})");
        ClientLog.Line($"[3] Publisher опубликовал его, {created.Publication}");
        ClientLog.Line($"[4] Broker принял событие и определил подписок: {Join(created.Subscribers.Select(subscriber =>
            $"{subscriber.Broker} → {subscriber.Subscriber}"))}");
        ClientLog.Line($"[5] событие получили {Count(created, "SUCCESS")} Subscriber: {Join(Names(created))}");
        ClientLog.Line($"[6] один Broker отдаёт событие нескольким подписчикам, публикация заняла {created.PublishMs} мс, " +
                       $"обработка всех - {created.AllDoneMs} мс");

        ClientLog.Line($"[7] один Subscriber становится недоступен → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "NotificationService", "unavailable", reason: "notification service was stopped")).Message)}");

        seen = EventCount();
        Response payment = _client.SendRequest(RequestBuilder.BuildPayBooking(BookingId));
        EventTiming paid = LastOf(NewEvents(seen));

        ClientLog.Line($"[8] остальные продолжают работу: PaymentCompleted #{paid.EventId} обработали " +
                       $"{Join(Names(paid))}, недоступен = {Join(paid.Subscribers
                           .Where(subscriber => subscriber.Outcome != "SUCCESS")
                           .Select(subscriber => $"{subscriber.Subscriber} ({subscriber.Outcome})"))}");

        Response added = _client.SendRequest(RequestBuilder.BuildAddSubscriber(
            "ReportService", "logger", "broker-1", new[] { "BookingCancelled" }));
        ClientLog.Line($"[9] новый Subscriber подключается без изменения Publisher → {Shorten(added.Message)}");

        seen = EventCount();
        Response cancel = _client.SendRequest(RequestBuilder.BuildCancelBooking(BookingId));
        EventTiming cancelled = LastOf(NewEvents(seen));

        ClientLog.Line($"[10] новый Subscriber получает BookingCancelled #{cancelled.EventId}: " +
                       $"{Join(Names(cancelled))} → {Shorten(cancel.Message)}");

        ClientLog.Line($"      Publisher не менялся: публикаций = {Status()?.PublishedEvents}, " +
                       $"подписчиков = {Subscribers().Count}");

        _client.SendRequest(RequestBuilder.BuildRestoreSubscriber("NotificationService"));
        PrintTable(DeliveryHeader, DeliveryOf(NewEvents(start)));
        return true;
    }

    // --------------------------------------- ЧАСТЬ 5: НЕСКОЛЬКО BROKER, ОЧЕРЕДЬ, ПОВТОРЫ

    public bool RunPubSubBrokers(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line($"=== LR6 PART 5: TWO BROKERS, EVENT QUEUE AND REDELIVERY, room {roomId} ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();

        int scenario = EventCount();

        ClientLog.Line($"[1] Broker настроено {Brokers().Count}: " +
                       $"{Join(Brokers().Select(broker => $"{broker.Name} = {broker.State}, подписчиков {broker.Subscribers.Count}"))}");
        PrintSubscriptions();

        ClientLog.Line("[2] временное хранение: подписчик упал до доставки, событие остаётся в очереди Broker, а не теряется");
        ClientLog.Line($"    отказ AnalyticsService → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "AnalyticsService", "unavailable", reason: "analytics is down")).Message)}");

        int seen = EventCount();
        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;

        foreach (EventTiming entry in NewEvents(seen))
        {
            ClientLog.Line($"[3] {entry.EventType} #{entry.EventId}: доставлен в {Count(entry, "SUCCESS")} подписчик(а), " +
                           $"не доставлено = {entry.Subscribers.Count - Count(entry, "SUCCESS")}");

            foreach (SubscriberTiming subscriber in entry.Subscribers.Where(subscriber => subscriber.Outcome != "SUCCESS"))
                ClientLog.Line($"    {subscriber.Broker} → QUEUE → {subscriber.Subscriber}: {subscriber.Outcome}, " +
                               $"событие сохранено в очереди Broker-а");
        }

        ClientLog.Line($"[4] подписчик восстановлен → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreSubscriber("AnalyticsService")).Message)}");
        ClientLog.Line($"    повторная доставка → {Shorten(_client.SendRequest(RequestBuilder.BuildRetryPending()).Message)}");

        ClientLog.Line("[5] три попытки доставки: Attempt 1 → FAIL, Attempt 2 → FAIL, Attempt 3 → SUCCESS");
        ClientLog.Line($"    подписчик падает на двух попытках → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "AnalyticsService", "processing", attempts: 2, reason: "handler fails twice, then recovers")).Message)}");

        seen = EventCount();
        _client.SendRequest(RequestBuilder.BuildPublishEvent("PaymentCompleted", BookingId,
            data: new Dictionary<string, string> { ["amount"] = "200.00" }));

        EventTiming attempts = LastOf(NewEvents(seen));
        ClientLog.Line($"    Attempt 1 → FAIL ({OutcomeOf(attempts, "AnalyticsService")}, " +
                       $"{attempts.EventType} #{attempts.EventId} ушёл в очередь broker-2)");

        ClientLog.Line($"    повторная доставка → {Shorten(_client.SendRequest(RequestBuilder.BuildRetryPending()).Message)}");

        foreach (SubscriberTiming subscriber in Latest().First(entry => entry.EventId == attempts.EventId).Subscribers)
            ClientLog.Line($"      Attempt {subscriber.Attempts} → {(subscriber.Outcome == "SUCCESS" ? "SUCCESS" : "FAIL")}, " +
                           $"{subscriber.Broker} → {subscriber.Subscriber}: результат = {subscriber.Outcome}, " +
                           $"обработка = {subscriber.ProcessingMs} мс");

        ClientLog.Line($"[6] AnalyticsService собирает статистику и не меняет данные предметной области → " +
                       $"{Shorten(_client.SendRequest(RequestBuilder.BuildEventStatistics()).Message)}");

        ClientLog.Line("[7] несколько клиентов одновременно: события каждого клиента доставляются под своим идентификатором клиента");
        seen = EventCount();
        MultiClientReserve(roomId);

        foreach (EventTiming entry in NewEvents(seen))
            ClientLog.Line($"    {entry.ClientId} → {entry.EventType} #{entry.EventId} → {Join(Names(entry))}");

        ClientLog.Line("[8] анализ задержек при разных искусственных задержках подписчиков:");
        PrintTimings(NewEvents(scenario));

        ClientLog.Line($"[9] отказ одного Broker → {Shorten(_client.SendRequest(
            RequestBuilder.BuildFailBroker("broker-1", "broker process stopped")).Message)}");

        seen = EventCount();
        Response published = _client.SendRequest(RequestBuilder.BuildPublishEvent("PaymentCompleted", BookingId,
            data: new Dictionary<string, string> { ["amount"] = "200.00" }));
        ClientLog.Line($"    событие всё равно опубликовано через живой Broker → {Shorten(published.Message)}");

        foreach (EventTiming entry in NewEvents(seen))
            ClientLog.Line($"    принят: {entry.Publication}, доставка = {Join(Names(entry))}");

        ClientLog.Line($"[10] Broker восстановлен → {Shorten(_client.SendRequest(RequestBuilder.BuildRestoreBroker("broker-1")).Message)}");
        ClientLog.Line($"     итог: {Status()?.Summary}");
        return true;
    }

    // ----------------------------------------- ЧАСТЬ 4, ТРЕБОВАНИЕ 5: 13 СИТУАЦИЙ ОШИБОК

    public bool RunPubSubErrors(int roomId)
    {
        ClientLog.Line("");
        ClientLog.Line("=== LR6 PART 4, REQUIREMENT 5: 13 ERROR SITUATIONS ===");

        if (!EnsurePubSubMode())
            return false;

        ResetDatabase();
        ResetPubSub();

        ClientLog.Line($"1.  сервер недоступен                 → {OfflineServer().Message}");

        ClientLog.Line($"2.  Broker недоступен                 → {Shorten(_client.SendRequest(
            RequestBuilder.BuildFailBroker("broker-1", "broker is down")).Message)} | {Shorten(_client.SendRequest(
            RequestBuilder.BuildFailBroker("broker-2", "broker is down")).Message)}");

        int refusedBefore = Status()?.FailedPublications ?? 0;
        ClientLog.Line($"3.  Publisher не может опубликовать   → {Shorten(_client.SendRequest(RequestBuilder.BuildPublishEvent(
            "BookingCreated", 1, roomId: "101")).Message)}");
        ClientLog.Line($"    сервер жив, отказов публикации  = {Status()?.FailedPublications} " +
                       $"(до попытки = {refusedBefore})");

        ClientLog.Line($"    Broker восстановлен               → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreBroker("broker-1")).Message)}");
        ClientLog.Line($"    живой Broker всё равно принял     → {Shorten(_client.SendRequest(RequestBuilder.BuildPublishEvent(
            "PaymentCompleted", 1)).Message)}");
        ClientLog.Line($"    broker-2 восстановлен             → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreBroker("broker-2")).Message)}");

        ClientLog.Line($"4.  Subscriber недоступен             → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "Logger", "unavailable", reason: "logger stopped")).Message)}");
        ClientLog.Line($"5.  ошибка соединения с Subscriber    → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "RoomService", "connection", attempts: 2, reason: "connection was reset")).Message)}");
        ClientLog.Line($"6.  Subscriber не зарегистрирован     → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "GhostService", "unavailable")).Message)}");
        ClientLog.Line($"7.  нет подписки на тип события       → {Shorten(_client.SendRequest(RequestBuilder.BuildUnsubscribeEvent(
            "RoomService", "PaymentCompleted")).Message)}");
        ClientLog.Line($"8.  неизвестный тип события           → {Shorten(_client.SendRequest(RequestBuilder.BuildPublishEvent(
            "RoomCleaned", 1)).Message)}");
        ClientLog.Line($"9.  ошибка обработки события          → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "NotificationService", "processing", attempts: 1, reason: "handler threw")).Message)}");

        int seen = EventCount();
        Response booking = _client.SendRequest(RequestBuilder.BuildCreateBooking(
            roomId, DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1));
        BookingId = Parse(booking)?.BookingId ?? BookingId;
        EventTiming created = LastOf(NewEvents(seen));

        ClientLog.Line($"10. ошибка доставки события           → {Count(created, "CONNECTION ERROR") + Count(created, "PROCESSING ERROR")} " +
                       $"доставок с ошибкой, {Count(created, "SUCCESS")} успешных (Event = {booking.EventTriggered})");

        ClientLog.Line($"    NotificationService восстановлен  → {Shorten(_client.SendRequest(
            RequestBuilder.BuildRestoreSubscriber("NotificationService")).Message)}");
        ClientLog.Line($"11. несколько Subscriber недоступны   → {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "Logger", "unavailable")).Message)} | {Shorten(_client.SendRequest(RequestBuilder.BuildFailSubscriber(
            "AnalyticsService", "connection")).Message)}");

        seen = EventCount();
        Response cancelled = _client.SendRequest(RequestBuilder.BuildPublishEvent("BookingCancelled", BookingId));
        ClientLog.Line($"    событие при недоступных подписчиках → {Shorten(cancelled.Message)}, доставлено = " +
                       $"{Count(LastOf(NewEvents(seen)), "SUCCESS")}, в очереди = {Status()?.Brokers.Sum(broker => broker.Pending) ?? 0}");

        ClientLog.Line($"12. некорректное событие              → {Shorten(_client.SendRequest(RequestBuilder.BuildPublishEvent(
            "BookingCreated", entityId: 0)).Message)}");
        ClientLog.Line($"13. некорректный запрос клиента       → {Shorten(_client.SendRequest(RequestBuilder.BuildInvalidRequest()).Message)} | " +
                       $"{_client.SendRequest(new Request { Operation = "AddSubscriber", Payload = "{ not json" }).Message}");
        ClientLog.Line($"    неизвестная операция              → {_client.SendRequest(new Request { Operation = "TeleportRoom", Payload = "{}" }).Message}");
        ClientLog.Line($"    дубликат подписки                 → {Shorten(_client.SendRequest(RequestBuilder.BuildSubscribeEvent(
            "Logger", "BookingCreated")).Message)}");

        ClientLog.Line("--- восстановление и повторная доставка ---");
        ClientLog.Line($"    подписчики подняты                → {Shorten(_client.SendRequest(RequestBuilder.BuildRestoreSubscriber("RoomService")).Message)} | " +
                       $"{Shorten(_client.SendRequest(RequestBuilder.BuildRestoreSubscriber("Logger")).Message)} | " +
                       $"{Shorten(_client.SendRequest(RequestBuilder.BuildRestoreSubscriber("AnalyticsService")).Message)}");
        ClientLog.Line($"    RetryPending                      → {Shorten(_client.SendRequest(RequestBuilder.BuildRetryPending()).Message)}");
        ClientLog.Line($"    сервер жив после всех ошибок      → операция без БД = {OfflineSafe().IsSuccess}, " +
                       $"публикаций = {Status()?.PublishedEvents}, отказов публикации = {Status()?.FailedPublications}");

        PrintSubscriptions();
        return true;
    }

    /// <summary>Все части подряд: используется скриптом прогонов и пунктом меню.</summary>
    public bool RunPubSubFinal(int roomId)
    {
        if (!EnsurePubSubMode())
            return false;

        return RunPubSubBasic()
            && RunPubSubSubscribers(roomId)
            && RunPubSubFailure(roomId)
            && RunPubSubNewSubscriber(roomId)
            && RunPubSubCycle(roomId)
            && RunPubSubBrokers(roomId)
            && RunPubSubErrors(roomId);
    }

    // ---------------------------------------------------------------------- УТИЛИТЫ

    private int BookingId { get; set; }

    /// <summary>
    /// Сценарии не наследуют состояние друг друга: Broker и все подписчики возвращаются в рабочее
    /// состояние, снятые и вооружённые ранее ошибки доставки тоже снимаются.
    /// </summary>
    private void ResetPubSub()
    {
        PubSubStatusReport? report = Status();

        if (report is null)
            return;

        foreach (BrokerStatus broker in report.Brokers)
            _client.SendRequest(RequestBuilder.BuildRestoreBroker(broker.Name));

        foreach (SubscriberStatus subscriber in report.Subscribers)
            _client.SendRequest(RequestBuilder.BuildRestoreSubscriber(subscriber.Name));

        ClientLog.Line($"[LR6] механизм приведён в рабочее состояние: Broker = {Join(report.Brokers.Select(broker => broker.Name))}, " +
                       $"Subscriber = {Join(report.Subscribers.Select(subscriber => subscriber.Name))}");
    }

    private PubSubStatusReport? Status() =>
        Read<PubSubStatusReport>(_client.SendRequest(RequestBuilder.BuildPubSubStatus()));

    private EventStatisticsReport? Statistics() =>
        Read<EventStatisticsReport>(_client.SendRequest(RequestBuilder.BuildEventStatistics()));

    private List<BrokerStatus> Brokers() => Status()?.Brokers ?? new List<BrokerStatus>();

    private List<SubscriberStatus> Subscribers() => Status()?.Subscribers ?? new List<SubscriberStatus>();

    private int EventCount() => Statistics()?.TotalEvents ?? 0;

    /// <summary>События, появившиеся после снимка: таблицы не разрастаются от эксперимента к эксперименту.</summary>
    private List<EventTiming> NewEvents(int seenBefore) =>
        Statistics()?.Events.Skip(seenBefore).ToList() ?? new List<EventTiming>();

    private List<EventTiming> Latest() => Statistics()?.Events ?? new List<EventTiming>();

    private static EventTiming LastOf(List<EventTiming> events) => events.LastOrDefault()!;

    private void Report(Response response) =>
        ClientLog.Line($"[LR6] {(response.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(response.Message)} " +
                       $"(Event = {response.EventTriggered})");

    private void PrintSubscriptions()
    {
        PubSubStatusReport? report = Status();

        if (report is null)
            return;

        PrintTable(SubscriptionHeader, report.Subscribers.Select(subscriber => new[]
        {
            subscriber.Broker,
            subscriber.Name,
            subscriber.Kind,
            Join(subscriber.Subscriptions),
            subscriber.State,
            subscriber.DelayMs.ToString(),
            subscriber.Handled.ToString(),
            subscriber.Failed.ToString(),
            subscriber.Pending.ToString()
        }).ToList());

        foreach (BrokerStatus broker in report.Brokers)
            ClientLog.Line($"[LR6] {broker.Name}: {broker.State}, подписчиков = {broker.Subscribers.Count}, " +
                           $"принято событий = {broker.Accepted}, всего попыток доставки = {broker.Attempts} " +
                           $"(лимит = {report.MaxDeliveryAttempts}), в очереди = {broker.Pending}" +
                           (broker.PendingEvents.Count == 0 ? string.Empty : $" ({Join(broker.PendingEvents)})"));
    }

    /// <summary>Строки таблицы доставки: по одной на каждую пару «событие → подписчик».</summary>
    private static List<string[]> DeliveryOf(List<EventTiming> events)
    {
        var rows = new List<string[]>();
        int number = 0;

        foreach (EventTiming entry in events)
            foreach (SubscriberTiming subscriber in entry.Subscribers)
            {
                number++;
                rows.Add(new[]
                {
                    number.ToString(),
                    entry.ClientId,
                    $"#{entry.EventId}",
                    entry.EventType,
                    subscriber.Broker,
                    subscriber.Subscriber,
                    subscriber.Outcome,
                    subscriber.Attempts.ToString(),
                    subscriber.ProcessingMs.ToString(),
                    Shorten(string.IsNullOrEmpty(subscriber.Error)
                        ? subscriber.Result
                        : $"{subscriber.Result} | {subscriber.Error}")
                });
            }

        return rows;
    }

    private void PrintTimings(List<EventTiming> events)
    {
        if (events.Count == 0)
            return;

        PrintTable(TimingHeader, events.Select(entry => new[]
        {
            $"{entry.EventType} #{entry.EventId}",
            entry.ClientId,
            entry.PublishMs.ToString(),
            entry.FirstDeliveryMs.ToString(),
            entry.MaxProcessingMs.ToString(),
            entry.AllDoneMs.ToString()
        }).ToList());
    }

    private static IEnumerable<string> Names(EventTiming? entry) =>
        entry?.Subscribers.Where(subscriber => subscriber.Outcome == "SUCCESS")
            .Select(subscriber => subscriber.Subscriber) ?? Enumerable.Empty<string>();

    private static int Count(EventTiming? entry, string outcome) =>
        entry?.Subscribers.Count(subscriber => subscriber.Outcome == outcome) ?? 0;

    /// <summary>Результат последней доставки события конкретным подписчиком.</summary>
    private static string OutcomeOf(EventTiming entry, string subscriber) =>
        entry.Subscribers.LastOrDefault(timing => timing.Subscriber == subscriber)?.Outcome ?? "нет доставки";

    private static string Join(IEnumerable<string> values) => string.Join(", ", values);

    private List<string> AllSubscriberNames() => Subscribers().Select(subscriber => subscriber.Name).ToList();

    /// <summary>Сравнение двух подходов по критериям из требования 2 Части 4.</summary>
    private static List<string[]> CouplingRows() => new()
    {
        new[]
        {
            "какие компоненты знают друг о друге",
            "Сервер знает каждый компонент и вызывает его напрямую: Logger, RoomService, NotificationService по типам и именам",
            "Сервер знает Publisher и Broker; Publisher не знает ни одного Subscriber"
        },
        new[]
        {
            "где находится логика маршрутизации",
            "В сервере: список вызовов зашит в код каждой операции",
            "В Broker: получатели выбираются по таблице подписок"
        },
        new[]
        {
            "насколько легко добавить новый компонент",
            "Нужно править сервер и пересобирать все операции",
            "Достаточно зарегистрировать Subscriber и подписку: Publisher и сервер не меняются",
        },
        new[]
        {
            "что произойдет при отказе одного Subscriber",
            "Операция падает или ждёт таймаута, остальные компоненты не получают вызов",
            "Broker фиксирует ошибку доставки, остальные Subscriber работают, событие ждёт в очереди"
        },
        new[]
        {
            "насколько сильно связаны компоненты",
            "Сильная связь: сервер компилируется со всеми компонентами",
            "Слабая связь (Loose coupling): общий контракт - только Event"
        }
    };

    /// <summary>Часть 5, Требование 5: несколько клиентов публикуют события одновременно.</summary>
    private void MultiClientReserve(int roomId)
    {
        ResetDatabase();

        int[] rooms = new[] { roomId, 102, 201 }.Distinct().ToArray();

        List<(string Who, TimedResponse Timed)> results = ConcurrentReserve(rooms[0], rooms.Length,
            number => RequestBuilder.BuildCreateBooking(
                rooms[(number - 1) % rooms.Length],
                DateTime.Today.AddDays(1), DateTime.Today.AddDays(3), guestId: 1 + (number - 1) % 3));

        foreach ((string who, TimedResponse timed) in results)
            ClientLog.Line($"    {who} → {(timed.Response.IsSuccess ? "SUCCESS" : "FAIL")}: {Shorten(timed.Response.Message)} " +
                           $"(Event = {timed.Response.EventTriggered})");
    }
}
