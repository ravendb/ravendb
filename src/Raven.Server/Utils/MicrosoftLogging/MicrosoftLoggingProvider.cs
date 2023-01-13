using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

[ProviderAlias("Sparrow")]
public class MicrosoftLoggingProvider : ILoggerProvider
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(MicrosoftLoggingProvider));

    public readonly LoggingSource LoggingSource;
    private readonly ServerNotificationCenter _notificationCenter;
    private readonly ConcurrentDictionary<string, SparrowLoggerWrapper> _loggers = new ConcurrentDictionary<string, SparrowLoggerWrapper>(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<StringSegment, LogLevel> _configuration = new ConcurrentDictionary<StringSegment, LogLevel>();

    public MicrosoftLoggingProvider(LoggingSource loggingSource, ServerNotificationCenter notificationCenter)
    {
        LoggingSource = loggingSource;
        _notificationCenter = notificationCenter;
    }

    public ILogger CreateLogger(string categoryName)
    {
        var lastDot = categoryName.LastIndexOf('.');
        (string source, string loggerName) = lastDot >= 0
            ? (categoryName[..lastDot], categoryName.Substring(lastDot + 1, categoryName.Length - lastDot - 1))
            : (categoryName, categoryName);
        var sparrowLogger = LoggingSource.GetLogger(source, loggerName);

        //Basically the logger is created only once but just for the safe side.
        return _loggers.GetOrAdd(categoryName, s => new SparrowLoggerWrapper(sparrowLogger)
        {
            MinLogLevel = GetLogLevelForCategory(categoryName)
        });
    }

    public void Dispose()
    {
    }

    public async Task InitAsync(string configurationPath, JsonOperationContext context)
    {
        Stream fileStream;
        try
        {
            fileStream = File.OpenRead(configurationPath);
        }
        catch (Exception e)
        {
            if (e is FileNotFoundException)
                return;

            var msg = $"Failed to open microsoft configuration file. FilePath:\"{configurationPath}\"";
            var alert = CreateAlert(msg, e);
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Add(alert));

            if (Logger.IsOperationsEnabled)
                Logger.Operations(msg, e);
            return;
        }
        await using (var configurationFile = fileStream)
        {
            await ReadAndApplyConfigurationAsync(configurationFile, context);
        }
    }

    private const string NotificationKey = "microsoft-configuration-logs-error";
    private const AlertType AlertType = NotificationCenter.Notifications.AlertType.MicrosoftLogsConfigurationLoadError;
    private readonly string _notificationId = AlertRaised.GetKey(AlertType, NotificationKey);

    public IEnumerable<(string Name, LogLevel MinLogLevel)> GetLoggers()
    {
        return _loggers.Select(x => (x.Key, x.Value.MinLogLevel));
    }
    public IEnumerable<(string Category, LogLevel LogLevel)> GetConfiguration()
    {
        return _configuration.Select(x => (x.Key.ToString(), x.Value));
    }
    public async Task ReadAndApplyConfigurationAsync(Stream streamConfiguration, JsonOperationContext context, bool reset = true)
    {
        try
        {
            if (reset)
                _configuration.Clear();

            await ReadConfigurationAsync(streamConfiguration, context);
            ApplyConfiguration();

            //If the code run on server startup the notification center is not initialized 
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Dismiss(_notificationId));
        }
        catch (Exception e)
        {
            var msg = $"Failed to init Microsoft log configuration. configuration content : {streamConfiguration}";
            var alert = CreateAlert(msg, e);

            //If the code run on server startup the notification center is not initialized 
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Add(alert));

            if (Logger.IsOperationsEnabled)
                Logger.Operations(msg, e);
        }
    }

    private static AlertRaised CreateAlert(string msg, Exception e)
    {
        return AlertRaised.Create(
            null,
            "Microsoft Logs Configuration Load Failed",
            msg,
            AlertType,
            NotificationSeverity.Warning,
            key: NotificationKey,
            details: new ExceptionDetails(e));
    }

    private async Task ReadConfigurationAsync(Stream configurationStr, JsonOperationContext context)
    {
        var blitConfiguration = await context.ReadForMemoryAsync(configurationStr, "logs/configuration");
        ReadConfiguration(blitConfiguration, null);
    }
    private void ReadConfiguration(BlittableJsonReaderObject jConfiguration, string rootCategory)
    {
        jConfiguration.BlittableValidation();
        foreach (var subCategory in jConfiguration.GetPropertyNames())
        {
            if (jConfiguration.TryGetWithoutThrowingOnError(subCategory, out LogLevel logLevel))
            {
                var category = subCategory == "LogLevel"
                    ? new StringSegment(rootCategory, 0, rootCategory.Length - 1)
                    : rootCategory + subCategory;
                _configuration[category] = logLevel;
            }
            else if (jConfiguration.TryGetWithoutThrowingOnError(subCategory, out BlittableJsonReaderObject jObjectValue))
            {
                ReadConfiguration(jObjectValue, rootCategory + subCategory + '.');
            }
            else
            {
                throw new InvalidOperationException($"Invalid value in microsoft configuration. Path {subCategory}, Value {jConfiguration[subCategory]}");
            }
        }
    }

    void ApplyConfiguration()
    {
        foreach (var (categoryName, logger) in _loggers)
        {
            logger.MinLogLevel = GetLogLevelForCategory(categoryName);
        }
    }

    private LogLevel GetLogLevelForCategory(string categoryName)
    {
        LogLevel logLevel;
        for (int i = categoryName.Length; i > 0; i = categoryName.LastIndexOf(".", i - 1, StringComparison.Ordinal))
        {
            var segment = new StringSegment(categoryName, 0, i);
            if (_configuration.TryGetValue(segment, out logLevel))
                return logLevel;
        }
        //Default configuration
        if (_configuration.TryGetValue(string.Empty, out logLevel))
            return logLevel;

        return LogLevel.Critical;
    }
}
