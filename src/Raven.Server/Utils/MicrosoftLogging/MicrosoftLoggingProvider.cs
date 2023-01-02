using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

[ProviderAlias("Sparrow")]
public class MicrosoftLoggingProvider : ILoggerProvider
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(MicrosoftLoggingProvider));

    private readonly LoggingSource _loggingSource;
    private readonly NotificationCenter.NotificationCenter _notificationCenter;
    private readonly ConcurrentDictionary<string, SparrowLoggerWrapper> _loggers = new ConcurrentDictionary<string, SparrowLoggerWrapper>(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<StringSegment, LogLevel> _configuration = new ConcurrentDictionary<StringSegment, LogLevel>();
    
    public MicrosoftLoggingProvider(LoggingSource loggingSource, NotificationCenter.NotificationCenter notificationCenter)
    {
        _loggingSource = loggingSource;
        _notificationCenter = notificationCenter;
    }

    public ILogger CreateLogger(string categoryName)
    {
        var lastDot = categoryName.LastIndexOf('.');
        (string source, string loggerName) = lastDot >= 0
            ? (categoryName[..lastDot], categoryName.Substring(lastDot + 1, categoryName.Length - lastDot - 1))
            : (categoryName, categoryName);
        var sparrowLogger = _loggingSource.GetLogger(source, loggerName);

        //Basically the logger is created only once but just for the safe side.
        return _loggers.GetOrAdd(categoryName, s => new SparrowLoggerWrapper(sparrowLogger)
        {
            MinLogLevel = GetLogLevelForCategory(categoryName)
        });
    }
    
    public void Dispose()
    {
    }

    public void Init(LogsConfiguration configuration)
    {
        var configurationStr = File.ReadAllText(configuration.MicrosoftLogsConfigurationPath.FullPath);
        SetConfiguration(configurationStr);
    }

    private const string NotificationKey = "microsoft-configuration-logs-error";
    private const AlertType AlertType = NotificationCenter.Notifications.AlertType.MicrosoftLogsConfigurationLoadError;
    private readonly string _notificationId = AlertRaised.GetKey(AlertType, NotificationKey);
    public void SetConfiguration(string strConfiguration, bool reset = true)
    {
        try
        {
            if(reset)
                _configuration.Clear();

            ReadConfiguration(strConfiguration);
            ApplyConfiguration();
            
            _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Dismiss(_notificationId));
        }
        catch (Exception e)
        {
            var msg = $"Failed to init Microsoft log configuration. configuration content : {strConfiguration}";
            var alert = AlertRaised.Create(
                null,
                "Microsoft Logs Configuration Load Failed",
                msg,
                AlertType,
                NotificationSeverity.Warning,
                key: NotificationKey,
                details: new ExceptionDetails(e));
            
            _notificationCenter.InitializeTask.ContinueWith(task => task.Result.Add(alert));
                
            if (Logger.IsInfoEnabled) 
                Logger.Info(msg, e);
        }
    }

    private void ReadConfiguration(string configurationStr)
    {
        var microsoftLogsConfiguration = JsonConvert.DeserializeObject<JObject>(configurationStr);
        ReadConfiguration(microsoftLogsConfiguration);
    }
    private void ReadConfiguration(JObject jConfiguration)
    {
        foreach (var (key, value) in jConfiguration.Value<JObject>())
        {
            if (value == null)
                continue;

            switch (value)
            {
                case JObject jObjectValue:
                    ReadConfiguration(jObjectValue);
                    break;
                case JValue jLogLevel:
                    if(jLogLevel.Type != JTokenType.String || Enum.TryParse(jLogLevel.Value<string>(), out LogLevel logLevel) == false)
                        goto default;
                    if (_configuration.TryAdd(jConfiguration.Path, logLevel) == false)
                        throw new InvalidOperationException($"Duplicate category - {jConfiguration.Path}");
                    break;
                default:
                    throw new InvalidOperationException($"Invalid value in microsoft configuration. Path {value.Path}, Value {value}");
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
