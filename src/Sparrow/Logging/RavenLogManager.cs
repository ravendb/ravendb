using System;

namespace Sparrow.Logging;

public sealed class RavenLogManager
{
    private static IRavenLogManager LogManager = RavenNullLogManager.Instance;

    internal static readonly RavenLogManager Instance = new();

    public bool IsAuditEnabled;

    private RavenLogManager()
    {
        Refresh();
    }

    internal void Shutdown()
    {
        LogManager.Shutdown();
    }

    internal IRavenLogger GetLogger(string name) => LogManager.GetLogger(name);

    internal IRavenLogger GetLogger(string name, Type loggerType) => LogManager.GetLogger(name);

    internal void Refresh()
    {
        var innerLogger = LogManager.GetLogger("Audit");
        IsAuditEnabled = innerLogger.IsInfoEnabled;

        LogManager.ConfigurationChanged += (_, _) => IsAuditEnabled = innerLogger.IsInfoEnabled;
    }

    public static void Set(IRavenLogManager logManager)
    {
        LogManager = logManager ?? throw new ArgumentNullException(nameof(logManager));

        Instance.Refresh();
    }
}
