using System.Text.Json;
using Shared.Network;

namespace Server;


/// Сервер фиксирует два штампа: момент приёма запроса и момент формирования ответа,

public static class TimeService
{
    public static async Task<Response> GetServerTimeAsync(string payload, string clientId)
    {
        ServerLog.Start(clientId, "TIME_REQUEST");

        DateTime receivedAt = DateTime.UtcNow;

        if (!TryReadDelay(payload, out int delayMs, out string error))
        {
            ServerLog.Fail(clientId, error);
            return new Response { IsSuccess = false, Message = error };
        }

        if (delayMs > 0)
            await Task.Delay(delayMs);

        DateTime serverTime = DateTime.UtcNow;

        ServerLog.Info(clientId,
            $"→ TIME_RESPONSE received={receivedTime(receivedAt)} sent={receivedTime(serverTime)} delay={delayMs}ms");

        return new Response
        {
            IsSuccess = true,
            Message = "Server time (UTC)",
            ServerReceivedTime = receivedAt,
            ServerTime = serverTime,
            EventTriggered = "TimeSynchronized",
            Data = JsonSerializer.Serialize(new
            {
                ServerTimeUtc = serverTime.ToString("O"),
                ServerReceivedUtc = receivedAt.ToString("O"),
                ServerProcessingDelayMs = delayMs
            })
        };
    }

    private static string receivedTime(DateTime utc) => utc.ToString("HH:mm:ss.fff");


    private static bool TryReadDelay(string payload, out int delayMs, out string error)
    {
        delayMs = ServerConfig.DefaultTimeResponseDelayMs;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(payload))
            return true;

        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(payload);
            root = doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            error = "Invalid time request payload: expected JSON object";
            return false;
        }

        if (!root.TryGetProperty("DelayMs", out var delayElem))
            return true;

        if (delayElem.ValueKind != JsonValueKind.Number || !delayElem.TryGetInt32(out delayMs))
        {
            delayMs = 0;
            error = "Invalid time data: DelayMs must be an integer number of milliseconds";
            return false;
        }

        if (delayMs < 0 || delayMs > ServerConfig.MaxTimeResponseDelayMs)
        {
            error = $"Invalid time data: DelayMs must be in [0, {ServerConfig.MaxTimeResponseDelayMs}]";
            return false;
        }

        return true;
    }
}
