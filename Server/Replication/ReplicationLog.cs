namespace Server.Replication;

public static class ReplicationLog
{
    public static void Check(string clientId, string label) =>
        ServerLog.Info(clientId, $"→ [INSTANCE CHECK] {label} → CHECK");

    public static void InstanceState(string clientId, string label, string state, string detail) =>
        ServerLog.Info(clientId, $"→ [INSTANCE CHECK] {label} → {state} ({detail})");

    public static void RequestDone(string clientId, string label, bool isSuccess) =>
        ServerLog.Info(clientId, $"→ [ROUTING] Request → {(isSuccess ? "SUCCESS" : "FAIL")} (instance = {label})");

    public static void InstanceUpdate(string clientId, string label) =>
        ServerLog.Info(clientId, $"→ [INSTANCE WRITE] {label} → UPDATE");

    public static void Failure(string clientId, string detail) =>
        ServerLog.Info(clientId, $"→ [FAILURE] {detail}");

    public static void Recovery(string clientId, string detail) =>
        ServerLog.Info(clientId, $"→ [RECOVERY] {detail}");

    public static void Failover(string clientId, string step) =>
        ServerLog.Info(clientId, $"→ [FAILOVER] {step}");

    public static void Redirected(string clientId, string target) =>
        ServerLog.Info(clientId, $"→ [ROUTING] request is redirected to {target}");

    public static void Replication(string clientId, string step) =>
        ServerLog.Info(clientId, $"→ [REPLICATION] {step}");

    public static void Replicated(string clientId, string source, string target, string detail) =>
        ServerLog.Info(clientId, $"→ [REPLICATION] SUCCESS — {source} → {target}: {detail}");

    public static void ReplicationFailed(string clientId, string error) =>
        ServerLog.Info(clientId, $"→ [REPLICATION] FAIL — {error}");

    public static void Synchronization(string clientId, string step) =>
        ServerLog.Info(clientId, $"→ [SYNC] {step}");

    public static void Compare(string clientId, string verdict) =>
        ServerLog.Info(clientId, $"→ [COMPARE] {verdict}");

    public static void Snapshot(string clientId, string label, string state) =>
        ServerLog.Info(clientId, $"→ [COPY {label}] {state}");
}
