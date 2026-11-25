namespace Shared.Network;

public class Request
{
    public string Operation { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;

    /// <summary>
    /// Имя, которым клиент представляется серверу, чтобы метки в журналах клиента и сервера совпадали
    /// (сервер без него нумерует соединения сам и не знает, какой это процесс клиента).
    /// </summary>
    public string ClientName { get; set; } = string.Empty;
}