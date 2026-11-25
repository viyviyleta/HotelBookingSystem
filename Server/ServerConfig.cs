namespace Server;

public static class ServerConfig
{
    public static int Port { get; set; } = 5005;

    public static int ReserveProcessingDelayMs { get; set; } = 1500;

    public static int DefaultTimeResponseDelayMs { get; set; } = 0;

    public const int MaxTimeResponseDelayMs = 10000;

    public static string SyncMode { get; set; } = "lock";

    /// <summary>ЛР №9: неконфиденциальный параметр из ConfigMap, видимый в журнале приложения.</summary>
    public static string AppMode { get; set; } = "default";

    /// <summary>Хранилище предметной области: "memory" (ЛР №2/№3) или "postgres" (ЛР №4).</summary>
    public static string Storage { get; set; } = "memory";

    public static bool UseDatabase => !Storage.Equals("memory", StringComparison.OrdinalIgnoreCase);

    /// <summary>Режим транзакции составной операции по умолчанию, если клиент его не указал.</summary>
    public static bool UseTransactionByDefault { get; set; } = true;

    /// <summary>Пауза между проверкой общего ресурса и записью в режиме без lock (воспроизведение гонки).</summary>
    public static int RaceWindowMs { get; set; } = 1000;

    /// <summary>ЛР №5: сервер ведёт две копии данных (Primary + Replica) и переключается на резервную при отказе.</summary>
    public static bool ReplicationEnabled { get; set; }

    /// <summary>ЛР №6: сервер публикует события предметной области в Broker, подписчики обрабатывают их асинхронно.</summary>
    public static bool PubSubEnabled { get; set; }

    /// <summary>
    /// ЛР №7: сервер поднимает кластер узлов, который выбирает лидера и согласовывает состояние
    /// предметной области большинством голосов.
    /// </summary>
    public static bool ConsensusEnabled { get; set; }

    /// <summary>Минимальное число узлов по заданию - 5 (Node 1 ... Node 5).</summary>
    public static int ClusterNodeCount { get; set; } = 5;

    /// <summary>Узлы слушают порты base..base+N-1 на 127.0.0.1.</summary>
    public static int ClusterBasePort { get; set; } = 5401;

    /// <summary>Сколько узел ждёт ответа соседа: нет ответа за это время - heartbeat timeout.</summary>
    public static int HeartbeatTimeoutMs { get; set; } = 1000;

    /// <summary>
    /// Искусственные задержки обработки (Часть 3, Требование 1): значения из методички 1/3/2 секунды.
    /// В тестах и в части экспериментов задержки уменьшаются, чтобы прогон не занимал минуты.
    /// </summary>
    public static Dictionary<string, int> SubscriberDelaysMs { get; } = new()
    {
        ["Logger"] = 1000,
        ["RoomService"] = 3000,
        ["NotificationService"] = 2000,
        ["AuditService"] = 1500,
        ["AnalyticsService"] = 800
    };

    /// <summary>Сколько попыток доставки Broker делает для одного подписчика, прежде чем событие будет потеряно.</summary>
    public static int EventDeliveryAttempts { get; set; } = 3;

    public static int EventWaitTimeoutMs { get; set; } = 20000;
}
