using System;
using NLog;
using Sparrow.Logging;

namespace Sparrow.Server.Logging;

public sealed class RavenNLogLogManager : IRavenLogManager
{
    public static readonly RavenNLogLogManager Instance = new();

    private RavenNLogLogManager()
    {
        LogManager.ConfigurationChanged += (sender, args) =>
        {
            ConfigurationChanged?.Invoke(sender, new RavenNLogLoggingConfigurationChangedEventArgs(args));
        };
    }

    public IRavenLogger GetLogger(string name) => new RavenLogger(LogManager.GetLogger(name));

    public event EventHandler<RavenLoggingConfigurationChangedEventArgs> ConfigurationChanged;

    public void Shutdown()
    {
        LogManager.Shutdown();
    }
}
