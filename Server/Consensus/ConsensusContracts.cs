namespace Server.Consensus;

/// <summary>Запуск выборов: reason попадает в журнал, чтобы было видно, почему выборы начались.</summary>
public sealed class ElectionCommand
{
    public string Reason { get; set; } = "manual request";
}

/// <summary>Отказ узла способом из методички: state, stalled, exception, silent или offline.</summary>
public sealed class FailNodeCommand
{
    public int Node { get; set; }

    public string Mode { get; set; } = "state";

    public string Reason { get; set; } = string.Empty;
}

/// <summary>Восстановление узла. В списке узлов он остаётся, но роль получает заново на выборах.</summary>
public sealed class RestoreNodeCommand
{
    public int Node { get; set; }

    public string Reason { get; set; } = string.Empty;
}

/// <summary>
/// Предложение изменения. Для варианта №2 Subject = BookingStatus, Value = Paid или Cancelled,
/// EntityId - идентификатор брони. ConfigurationVersion используется как значение вне предметной области.
/// </summary>
public sealed class ProposeCommand
{
    public string Subject { get; set; } = "BookingStatus";

    public string Value { get; set; } = string.Empty;

    public int EntityId { get; set; }

    /// <summary>Узлы, которые должны проголосовать NO: нужно для показа разделившихся голосов.</summary>
    public List<int> Oppose { get; set; } = [];
}

/// <summary>Одно сообщение между узлами: эксперименты и ситуации «ошибочное сообщение».</summary>
public sealed class NodeMessageCommand
{
    public int From { get; set; }

    public int To { get; set; }

    public string Type { get; set; } = MessageTypes.Heartbeat;
}

/// <summary>Параллельная рассылка: проверка, что узел принимает несколько сообщений одновременно.</summary>
public sealed class BroadcastCommand
{
    public string Type { get; set; } = MessageTypes.Status;
}

public sealed class JournalCommand
{
    public int Lines { get; set; } = 14;
}
