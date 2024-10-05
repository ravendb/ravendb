using System;

namespace Sparrow.Logging;

internal static class RavenLogManagerSparrowExtensions
{
    public static IRavenLogger GetLoggerForSparrow<T>(this RavenLogManager logManager) => GetLoggerForSparrow(logManager, typeof(T));

    public static IRavenLogger GetLoggerForSparrow(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return logManager.GetLogger(type.FullName)
            .WithProperty(Global.Constants.Logging.Properties.Resource, "Sparrow");
    }
}
