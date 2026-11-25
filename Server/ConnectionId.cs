namespace Server;

/// <summary>
/// Идентификатор соединения. Сервер нумерует подключения сам, но для журналов нужно имя,
/// которое прислал клиент: иначе метки в логах сервера и клиента не совпадают.
/// Имя принимается один раз — при первом запросе соединения.
/// </summary>
public sealed class ConnectionId
{
    public ConnectionId(string fallbackName)
    {
        FallbackName = fallbackName;
        Name = fallbackName;
    }

    public string Name { get; private set; }

    /// <summary>Имя, которое сервер выдал сам (нумерация подключений), пока клиент не представился.</summary>
    public string FallbackName { get; }

    public bool Announced { get; private set; }

    /// <summary>Возвращает true, если имя клиента отличается от выданного сервером и было принято.</summary>
    public bool Announce(string? clientName)
    {
        if (Announced || string.IsNullOrWhiteSpace(clientName))
            return false;

        Announced = true;

        if (clientName == Name)
            return false;

        Name = clientName;
        return true;
    }

    public override string ToString() => Name;
}
