using System;
using NLog;
using Sparrow.Logging;

namespace Raven.Client.Logging;

internal static class RavenLogManagerClientExtensions
{
    public static RavenLogger GetLoggerForClient<T>(this RavenLogManager logManager) => GetLoggerForClient(logManager, typeof(T));

    public static RavenLogger GetLoggerForClient(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Client"));
    }
}
