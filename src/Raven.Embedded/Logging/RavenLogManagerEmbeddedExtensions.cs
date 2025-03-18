using System;
using Sparrow.Logging;

namespace Raven.Embedded.Logging;

internal static class RavenLogManagerEmbeddedExtensions
{
    public static IRavenLogger GetLoggerForEmbedded<T>(this RavenLogManager logManager) => GetLoggerForEmbedded(logManager, typeof(T));

    public static IRavenLogger GetLoggerForEmbedded(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return logManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Embedded");
    }
}
