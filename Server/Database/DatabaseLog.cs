namespace Server.Database;

public static class DatabaseLog
{
    public static void Mode(string clientId, string mode) =>
        ServerLog.Info(clientId, $"→ [DB MODE] {mode}");

    public static void Begin(string clientId) =>
        ServerLog.Info(clientId, "→ [DB] BEGIN TRANSACTION");

    public static void Commit(string clientId) =>
        ServerLog.Info(clientId, "→ [DB] COMMIT → changes saved");

    public static void Rollback(string clientId, string reason) =>
        ServerLog.Info(clientId, $"→ [DB] ROLLBACK → changes cancelled ({reason})");

    public static void Autocommit(string clientId, string statement) =>
        ServerLog.Info(clientId, $"→ [DB] {statement} → applied immediately (autocommit, no transaction)");

    public static void Statement(string clientId, string statement, string result) =>
        ServerLog.Info(clientId, $"→ [DB] {statement} → {result}");

    public static void Operation(string clientId, int number, string name, string result) =>
        ServerLog.Info(clientId, $"→ [OP{number}] {name} → {result}");

    public static void Check(string clientId, string state) =>
        ServerLog.Info(clientId, $"→ [DB CHECK] {state}");

    public static void Error(string clientId, string error) =>
        ServerLog.Info(clientId, $"→ [DB ERROR] {error}");

    public static void State(string clientId, string state) =>
        ServerLog.Info(clientId, $"→ [DB STATE] {state}");
}
