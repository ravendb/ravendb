using System;
using NLog;

namespace Sparrow.Logging;

internal static class RavenLogManagerSparrowExtensions
{
    public static RavenLogger GetLoggerForSparrow<T>(this RavenLogManager logManager) => GetLoggerForSparrow(logManager, typeof(T));

    public static RavenLogger GetLoggerForSparrow(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Global.Constants.Logging.Properties.Resource, "Sparrow"));
    }
}
