namespace Shared.Configuration;

/// <summary>
/// ЛР №8: разбор переменных окружения контейнера. Общее для сервера и клиента,
/// чтобы одинаковое значение одинаково понималось обоими компонентами.
/// Неразобранное значение - не ошибка запуска: вызывающий оставляет прежнее значение.
/// </summary>
public static class Env
{
    /// <summary>Целое в допустимых границах: «abc», пустая строка и выход за границы - не число.</summary>
    public static bool TryNumber(string? value, int min, int max, out int number)
    {
        if (int.TryParse(value, out number) && number >= min && number <= max)
            return true;

        number = 0;
        return false;
    }

    /// <summary>Выключатель режима: on/off, true/false, yes/no, 1/0. Всё остальное - не переключатель.</summary>
    public static bool TryFlag(string? value, out bool enabled)
    {
        switch (value?.Trim().ToLowerInvariant())
        {
            case "on" or "true" or "yes" or "1":
                enabled = true;
                return true;
            case "off" or "false" or "no" or "0":
                enabled = false;
                return true;
            default:
                enabled = false;
                return false;
        }
    }

    /// <summary>Строка конфигурации: пробелы по краям не значат, пустое значение оставляет прежнее.</summary>
    public static string Text(string? value, string fallback) =>
        string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
}