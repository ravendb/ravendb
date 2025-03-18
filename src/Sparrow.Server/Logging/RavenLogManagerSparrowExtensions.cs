using System;
using NLog;
using Sparrow.Logging;

namespace Sparrow.Server.Logging;

internal static class RavenLogManagerSparrowServerExtensions
{
    public static RavenLogger GetLoggerForSparrowServer<T>(this RavenLogManager logManager) => GetLoggerForSparrowServer(logManager, typeof(T));

    public static RavenLogger GetLoggerForSparrowServer(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Sparrow"));
    }
}
