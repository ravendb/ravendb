using System;
using NLog.Config;
using Sparrow.Logging;

namespace Sparrow.Server.Logging;

public class RavenNLogLoggingConfigurationChangedEventArgs : RavenLoggingConfigurationChangedEventArgs
{
    public readonly LoggingConfigurationChangedEventArgs Arguments;

    public RavenNLogLoggingConfigurationChangedEventArgs(LoggingConfigurationChangedEventArgs arguments)
    {
        Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
    }
}
