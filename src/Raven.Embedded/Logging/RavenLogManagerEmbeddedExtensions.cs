using System;
using NLog;
using Sparrow.Logging;

namespace Raven.Embedded.Logging;

internal static class RavenLogManagerEmbeddedExtensions
{
    public static RavenLogger GetLoggerForEmbedded<T>(this RavenLogManager logManager) => GetLoggerForEmbedded(logManager, typeof(T));

    public static RavenLogger GetLoggerForEmbedded(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Embedded"));
    }
}
