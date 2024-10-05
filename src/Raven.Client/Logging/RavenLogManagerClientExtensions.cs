using System;
using Sparrow.Logging;

namespace Raven.Client.Logging;

internal static class RavenLogManagerClientExtensions
{
    public static IRavenLogger GetLoggerForClient<T>(this RavenLogManager logManager) => GetLoggerForClient(logManager, typeof(T));

    public static IRavenLogger GetLoggerForClient(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return logManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Client");
    }
}
