using System;
using NLog;
using Sparrow.Global;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Voron.Logging;

internal static class RavenLogManagerVoronExtensions
{
    public static RavenLogger GetLoggerForGlobalVoron<T>(this RavenLogManager logManager) => GetLoggerForGlobalVoron(logManager, typeof(T));

    public static RavenLogger GetLoggerForGlobalVoron(this RavenLogManager logManager, Type type)
    {
        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, LoggingResource.Voron));
    }

    public static RavenLogger GetLoggerForVoron<T>(this RavenLogManager logManager, StorageEnvironmentOptions options, string filePath) => GetLoggerForVoron(logManager, typeof(T), options, filePath);

    public static RavenLogger GetLoggerForVoron(this RavenLogManager logManager, Type type, StorageEnvironmentOptions options, string filePath)
    {
        if (options == null) 
            throw new ArgumentNullException(nameof(options));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, options.LoggingResource)
            .WithProperty(Constants.Logging.Properties.Component, options.LoggingComponent)
            .WithProperty(Constants.Logging.Properties.Data, filePath));
    }
}
