using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Utils.MicrosoftLogging;

public class MicrosoftLoggingConfiguration : IEnumerable<(string Category, LogLevel LogLevel)>
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(MicrosoftLoggingConfiguration));
    private const string NotificationKey = "microsoft-configuration-logs-error";
    private const AlertType AlertType = NotificationCenter.Notifications.AlertType.MicrosoftLogsConfigurationLoadError;

    private readonly string _notificationId = AlertRaised.GetKey(AlertType, NotificationKey);

    private readonly IEnumerable<KeyValuePair<string, SparrowLoggerWrapper>> _loggers;
    private readonly NotificationCenter.NotificationCenter _notificationCenter;
    private readonly ConcurrentDictionary<StringSegment, LogLevel> _configuration = new ConcurrentDictionary<StringSegment, LogLevel>();

    public MicrosoftLoggingConfiguration(IEnumerable<KeyValuePair<string, SparrowLoggerWrapper>> loggers, NotificationCenter.NotificationCenter notificationCenter)
    {
        _loggers = loggers;
        _notificationCenter = notificationCenter;
    }

    public LogLevel GetLogLevelForCategory(string categoryName)
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

        return LogLevel.None;
    }

    public IEnumerator<(string Category, LogLevel LogLevel)> GetEnumerator()
    {
        foreach (var (category, logLevel) in _configuration)
        {
            yield return (Category: category.ToString(), LogLevel: logLevel);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void ReadConfiguration(string configurationPath, JsonOperationContext context, bool shouldThrow, bool reset = true)
    {
        FileStream stream;
        try
        {
            stream = File.Open(configurationPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        }
        catch (Exception e)
        {
            if (e is FileNotFoundException)
            {
                //If the code run on server startup the notification center is not initialized 
                _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Dismiss(_notificationId));
                return;
            }

            var msg = $"Failed to open microsoft configuration file. FilePath:\"{configurationPath}\"";
            var alert = CreateAlert(msg, e);
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Add(alert));

            if (Logger.IsOperationsEnabled)
                Logger.Operations(msg, e);

            if (shouldThrow)
                throw;
            return;
        }
        using (stream)
        {
            ReadConfiguration(stream, context, reset);
        }
    }

    public void ReadConfiguration(Stream streamConfiguration, JsonOperationContext context, bool reset = true)
    {
        BlittableJsonReaderObject blitConfiguration = null;
        try
        {
            if (reset)
                _configuration.Clear();

            blitConfiguration = context.Sync.ReadForMemory(streamConfiguration, "logs/configuration");
            ReadConfiguration(blitConfiguration, null);

            //If the code run on server startup the notification center is not initialized 
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Dismiss(_notificationId));
        }
        catch (Exception e)
        {
            HandleReadConfigurationFailure(blitConfiguration, e);
        }
    }

    public void ReadConfiguration(BlittableJsonReaderObject blitConfiguration, bool reset = true)
    {
        try
        {
            if (reset)
                _configuration.Clear();

            ReadConfiguration(blitConfiguration, null);

            //If the code run on server startup the notification center is not initialized 
            _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Dismiss(_notificationId));
        }
        catch (Exception e)
        {
            HandleReadConfigurationFailure(blitConfiguration, e);
        }
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

    private void HandleReadConfigurationFailure(BlittableJsonReaderObject blitConfiguration, Exception e)
    {
        var msg = $"Failed to read and apply Microsoft log configuration.";

        try
        {
            if (blitConfiguration != null)
                msg = $"{msg} Configuration content : {blitConfiguration}";
        }
        catch
        {
            // ignored
        }

        var alert = CreateAlert(msg, e);

        //If the code run on server startup the notification center is not initialized 
        _ = _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Add(alert));

        if (Logger.IsOperationsEnabled)
            Logger.Operations(msg, e);
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
}
