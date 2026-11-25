using Server;
using Xunit;

namespace Tests;

/// <summary>
/// ЛР №9, требование 6: параметр ConfigMap APP_MODE читается из окружения,
/// пустое значение заменяется режимом по умолчанию, пробелы по краям отбрасываются.
/// </summary>
public class AppModeTests
{
    [Fact]
    public void MissingAppMode_UsesDefault()
    {
        Assert.Equal("default", EnvironmentConfig.ResolveAppMode(null));
        Assert.Equal("default", EnvironmentConfig.ResolveAppMode(""));
        Assert.Equal("default", EnvironmentConfig.ResolveAppMode("   "));
    }

    [Fact]
    public void AppMode_IsTrimmed()
    {
        Assert.Equal("kubernetes", EnvironmentConfig.ResolveAppMode(" kubernetes "));
    }

    [Fact]
    public void ReadAppMode_ReadsEnvironmentVariable()
    {
        string? previous = Environment.GetEnvironmentVariable("APP_MODE");

        try
        {
            Environment.SetEnvironmentVariable("APP_MODE", "kubernetes");

            Assert.Equal("kubernetes", EnvironmentConfig.ReadAppMode());
        }
        finally
        {
            Environment.SetEnvironmentVariable("APP_MODE", previous);
        }
    }
}
