using Shared.Network;

namespace Server;


public interface IBookingApi
{
    string SyncMechanism { get; }

    Task<Response> CheckAvailabilityAsync(string payload, string clientId);

    Task<Response> CreateBookingAsync(string payload, string clientId);

    Response PayBooking(string payload, string clientId);

    Response CancelBooking(string payload, string clientId);
}

public interface IDatabaseBookingApi : IBookingApi
{
    Response DatabaseSnapshot(string payload, string clientId);

    Response ResetDatabase(string payload, string clientId);

    Response CommitFailureCheck(string payload, string clientId);

    Response DatabaseHealth(string payload, string clientId);
}

/// <summary>Операции репликации и отказоустойчивости (ЛР №5): состояние копий, отказ, восстановление, сверка.</summary>
public interface IReplicationBookingApi : IDatabaseBookingApi
{
    Response InstanceStatus(string payload, string clientId);

    Response FailInstance(string payload, string clientId);

    Response RestoreInstance(string payload, string clientId);

    Response CompareReplicas(string payload, string clientId);

    Response SynchronizeReplicas(string payload, string clientId);
}

/// <summary>
/// Операции модели Pub/Sub (ЛР №6): подписчики и их подписки, состояние Broker, ручная публикация
/// события, повторная доставка и статистика задержек. Отдельный интерфейс потому, что события живут
/// поверх любого режима хранения - и с одной копией данных, и с двумя.
/// </summary>
public interface IPubSubBookingApi
{
    Response PubSubStatus(string payload, string clientId);

    Response PublishEvent(string payload, string clientId);

    Response AddSubscriber(string payload, string clientId);

    Response SubscribeEvent(string payload, string clientId);

    Response UnsubscribeEvent(string payload, string clientId);

    Response FailSubscriber(string payload, string clientId);

    Response RestoreSubscriber(string payload, string clientId);

    Response FailBroker(string payload, string clientId);

    Response RestoreBroker(string payload, string clientId);

    Response RetryPending(string payload, string clientId);

    Response EventStatistics(string payload, string clientId);
}

/// <summary>
/// Операции кластера (ЛР №7): состояние узлов, выборы лидера, heartbeat-проверка доступности,
/// отказ и восстановление узла, предложение значения, голосование и журнал. Отдельный интерфейс
/// потому, что консенсус надстраивается над теми же операциями предметной области, что и в ЛР №4-№6.
/// </summary>
public interface IConsensusBookingApi
{
    Response ClusterStatus(string payload, string clientId);

    Response StartElection(string payload, string clientId);

    Response HeartbeatProbe(string payload, string clientId);

    Response FailNode(string payload, string clientId);

    Response RestoreNode(string payload, string clientId);

    Response ProposeChange(string payload, string clientId);

    Response SendNodeMessage(string payload, string clientId);

    Response BroadcastNodes(string payload, string clientId);

    Response ClusterJournal(string payload, string clientId);

    Response ClusterStatistics(string payload, string clientId);
}
