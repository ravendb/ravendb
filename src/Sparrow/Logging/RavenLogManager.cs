using NLog;

namespace Sparrow.Logging;

internal class RavenLogManager
{
    public static readonly RavenLogManager Instance = new();

    public bool IsAuditEnabled;

    private RavenLogManager()
    {
        var innerLogger = LogManager.GetLogger("Audit");
        IsAuditEnabled = innerLogger.IsInfoEnabled;

        LogManager.ConfigurationChanged += (_, _) => IsAuditEnabled = innerLogger.IsInfoEnabled;
    }

    public void Shutdown()
    {
        LogManager.Shutdown();
    }

    public static RavenLogger CreateNullLogger()
    {
        return new RavenLogger(LogManager.CreateNullLogger());
    }
}
