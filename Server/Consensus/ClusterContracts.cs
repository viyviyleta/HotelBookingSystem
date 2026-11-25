using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Network;

namespace Server.Consensus;

/// <summary>Состояние узла: ровно две величины из методички - AVAILABLE и FAILED.</summary>
public enum NodeStatus
{
    Available,
    Failed
}

/// <summary>Роль узла в кластере.</summary>
public enum NodeRole
{
    Follower,
    Leader
}

/// <summary>
/// Способ смоделировать отказ. Первые четыре - способы из теоретической части методички,
/// пятый добавлен для случая «невозможно связаться с узлом» из Требования 2.
/// </summary>
public enum NodeFault
{
    /// <summary>Узел работает.</summary>
    None,

    /// <summary>Изменение состояния: узел сам сообщает, что он FAILED.</summary>
    State,

    /// <summary>Остановка обработки: соединение принимается, ответ не формируется никогда.</summary>
    Stalled,

    /// <summary>Искусственное исключение при обработке сообщения.</summary>
    Exception,

    /// <summary>Прекращение ответа на heartbeat: обычные сообщения узел ещё обрабатывает.</summary>
    Silent,

    /// <summary>Узел выключен: сокет-слушатель остановлен, соединение отклоняется.</summary>
    Offline
}

/// <summary>
/// Виды сообщений между узлами. Запросы узел принимает, ответы только отправляет:
/// разделение нужно, чтобы ошибочное «ответное» сообщение не выглядело как настоящий запрос.
/// </summary>
public static class MessageTypes
{
    public const string Heartbeat = "HEARTBEAT";
    public const string Alive = "ALIVE";
    public const string Election = "ELECTION";
    public const string Leader = "LEADER";
    public const string Propose = "PROPOSE";
    public const string Vote = "VOTE";
    public const string Commit = "COMMIT";
    public const string Status = "STATUS";

    /// <summary>Запросы, на которые узел отвечает.</summary>
    public static readonly IReadOnlyList<string> Requests = [Heartbeat, Election, Leader, Propose, Commit, Status];

    /// <summary>Ответы узлов: heartbeat-подтверждение и голос.</summary>
    public static readonly IReadOnlyList<string> Replies = [Alive, Vote];

    public static readonly IReadOnlyList<string> Known = [.. Requests, .. Replies];

    public static bool IsRequest(string type) => Requests.Contains(type, StringComparer.OrdinalIgnoreCase);

    public static bool IsKnown(string type) => Known.Contains(type, StringComparer.OrdinalIgnoreCase);

    public static string Normalize(string type) =>
        Known.FirstOrDefault(known => known.Equals(type, StringComparison.OrdinalIgnoreCase)) ?? type;
}

/// <summary>Возможные итоги опроса узла: три ответа из методички плюс явный отказ голосовать.</summary>
public static class VoteResults
{
    public const string Yes = "YES";
    public const string No = "NO";
    public const string NoResponse = "NO RESPONSE";
    public const string Refused = "REFUSED";
}

/// <summary>
/// Сообщение между узлами. По протоколу это тот же TCP+JSON, что и в ЛР №4-№6: один JSON на строку,
/// envelope - Request/Response из Shared.Network, где Operation несёт тип сообщения.
/// </summary>
public sealed record ClusterMessage
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = string.Empty;

    [JsonPropertyName("from")]
    public int From { get; init; }

    [JsonPropertyName("to")]
    public int To { get; init; }

    /// <summary>Идентификатор раунда голосования, чтобы голоса одного предложения не путались с другим.</summary>
    [JsonPropertyName("proposal")]
    public string Proposal { get; init; } = string.Empty;

    /// <summary>Предлагаемое состояние предметной области: BookingStatus, ConfigurationVersion.</summary>
    [JsonPropertyName("subject")]
    public string Subject { get; init; } = string.Empty;

    [JsonPropertyName("value")]
    public string Value { get; init; } = string.Empty;

    /// <summary>Объект предметной области (идентификатор брони) для состояний из варианта №2.</summary>
    [JsonPropertyName("entityId")]
    public int EntityId { get; init; }

    /// <summary>Узлы, которые по условию эксперимента голосуют NO.</summary>
    [JsonPropertyName("oppose")]
    public IReadOnlyList<int> Oppose { get; init; } = [];

    [JsonPropertyName("reason")]
    public string Reason { get; init; } = string.Empty;

    /// <summary>Ответ узла опроснику: AVAILABLE или FAILED.</summary>
    [JsonPropertyName("status")]
    public string Status { get; init; } = string.Empty;

    [JsonPropertyName("role")]
    public string Role { get; init; } = string.Empty;

    /// <summary>Итог голосования узла: YES, NO, REFUSED.</summary>
    [JsonPropertyName("vote")]
    public string Vote { get; init; } = string.Empty;

    [JsonPropertyName("details")]
    public string Details { get; init; } = string.Empty;

    /// <summary>Сколько узлов знает этот узел - признак того, что таблица соседей живая.</summary>
    [JsonPropertyName("knownPeers")]
    public int KnownPeers { get; init; }

    /// <summary>Транзитный идентификатор соединения: в ответе показывает, кто именно ответил.</summary>
    [JsonPropertyName("echo")]
    public string Echo { get; init; } = string.Empty;

    public Request ToRequest() => new()
    {
        Operation = Type,
        ClientName = From > 0 ? $"Node {From}" : "Cluster",
        Payload = JsonSerializer.Serialize(this)
    };

    /// <summary>Разбор сообщения из envelope. Возвращает null, если JSON битый или не читается как сообщение.</summary>
    public static ClusterMessage? FromRequest(Request request)
    {
        try
        {
            ClusterMessage? message = JsonSerializer.Deserialize<ClusterMessage>(request.Payload);

            if (message is null)
                return null;

            return message with
            {
                Type = string.IsNullOrWhiteSpace(message.Type) ? request.Operation : message.Type,
                From = message.From > 0 ? message.From : ParseNodeId(request.ClientName)
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public Response ToResponse(bool isSuccess, string message) => new()
    {
        IsSuccess = isSuccess,
        Message = message,
        Data = JsonSerializer.Serialize(this)
    };

    public static int ParseNodeId(string who) =>
        who is not null && who.StartsWith("Node ", StringComparison.OrdinalIgnoreCase) &&
        int.TryParse(who[5..], out int id) ? id : 0;
}

/// <summary>Предложение лидера: какое состояние и какого объекта согласуется.</summary>
public sealed record Proposal(
    string Subject,
    string Value,
    int EntityId = 0,
    IReadOnlyList<int>? Oppose = null,
    string ClientId = "Client")
{
    /// <summary>Значение предметной области для варианта №2: BookingStatus конкретной брони.</summary>
    public string Description => EntityId > 0
        ? $"{Subject}={Value} (booking {EntityId})"
        : $"{Subject}={Value}";
}

/// <summary>Итог применения согласованного решения к предметной области.</summary>
public sealed record AppliedDecision(bool Applied, string Note, string EventName = "");

/// <summary>Ответ узла на опрос или голосование: одна строка таблицы heartbeat-раунда и одна строка таблицы голосов.</summary>
public sealed record NodeReply(
    int NodeId,
    string Node,
    string MessageType,
    string Result,
    string Status,
    string Role,
    int RoundTripMs,
    string Details,
    string Connection,
    int KnownPeers = 0);

/// <summary>Одна строка журнала кластера: время, узел, метка и текст.</summary>
public sealed record JournalEntry(string Time, string Who, string Marker, string Text)
{
    public override string ToString() => $"[{Time}] [{Who}] → [{Marker}] {Text}";
}

public sealed record NodeReport(
    int Id,
    string Name,
    int Port,
    string Status,
    string Role,
    string Fault,
    string Reason,
    int MessagesSent,
    int MessagesHandled,
    int HeartbeatsAnswered,
    int VotesCast,
    int TimeoutsSeen,
    int KnownPeers,
    IReadOnlyList<string> Peers,
    string LastLeaderSeen,
    string AgreedValue,
    string LastActivity);

public sealed record ClusterStatusReport(
    bool Enabled,
    string Summary,
    int Total,
    int Majority,
    int Available,
    string Leader,
    string ElectionReason,
    int Elections,
    int Proposals,
    int ConsensusAchieved,
    int ConsensusRefused,
    int HeartbeatsSent,
    int Timeouts,
    IReadOnlyList<NodeReport> Nodes);

public sealed record HeartbeatReport(
    string Summary,
    string Initiator,
    IReadOnlyList<NodeReply> Exchanges,
    IReadOnlyList<string> FailedNodes,
    IReadOnlyList<JournalEntry> Journal);

public sealed record ElectionReport(
    string Summary,
    string Reason,
    string Initiator,
    int PreviousLeader,
    int LeaderId,
    string LeaderName,
    int Majority,
    int Available,
    int Total,
    bool SingleLeader,
    int DurationMs,
    IReadOnlyList<NodeReply> Polls,
    IReadOnlyList<string> Excluded,
    IReadOnlyList<JournalEntry> Journal);

public sealed record ConsensusReport(
    string Summary,
    string Proposal,
    string Description,
    string ProposedBy,
    int Majority,
    int Available,
    int Total,
    int Yes,
    int No,
    int NoResponse,
    int Refused,
    bool Achieved,
    bool Applied,
    string Decision,
    string AppliedNote,
    string EventName,
    int RoundMs,
    IReadOnlyList<NodeReply> Votes,
    IReadOnlyList<JournalEntry> Journal);

public sealed record ClusterStatisticsReport(
    string Summary,
    int Total,
    int Majority,
    int Elections,
    string CurrentLeader,
    int Proposals,
    int ConsensusAchieved,
    int ConsensusRefused,
    int HeartbeatsSent,
    int Timeouts,
    int MessagesSent,
    int MessagesAnswered,
    int VotesCast,
    IReadOnlyList<NodeReport> Nodes,
    IReadOnlyList<JournalEntry> Journal);

/// <summary>Сокет не поднимается: узел выключен или порт не принадлежит никакому узлу.</summary>
public sealed class NodeUnreachableException(int nodeId, string reason)
    : Exception($"node {nodeId} is unreachable: {reason}");

/// <summary>Узел принял соединение, но не ответил за отведённое время - это heartbeat timeout.</summary>
public sealed class NodeTimeoutException(int nodeId, int timeoutMs)
    : Exception($"node {nodeId} did not answer within {timeoutMs} ms (heartbeat timeout)");

/// <summary>Узел сам сообщает, что он FAILED, и потому не участвует в выборах и голосовании.</summary>
public sealed class NodeFailedException(int nodeId, string reason)
    : Exception($"node {nodeId} reports FAILED: {reason}");
