namespace Shared.Network;

public class Response
{
    public bool IsSuccess { get; set; }
    public string Message { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
    public string EventTriggered { get; set; } = string.Empty;

    // Часть 2 (синхронизация времени): сервер отдаёт два штампа, чтобы клиент
    // мог учесть задержку передачи. DateTime.MinValue означает «данных нет».
    public DateTime ServerReceivedTime { get; set; } = DateTime.MinValue;
    public DateTime ServerTime { get; set; } = DateTime.MinValue;

    public static Response Error(string message) => new() { IsSuccess = false, Message = message };
}
