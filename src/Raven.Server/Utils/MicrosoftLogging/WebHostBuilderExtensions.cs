using System;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class WebHostBuilderExtensions
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(WebHostBuilderExtensions));
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, ref LoggingSource loggingSource, LogsConfiguration configuration,
        NotificationCenter.NotificationCenter notificationCenter)
    {
        if (configuration.DisableMicrosoftLogs)
            return hostBuilder;
        
        string logPath = configuration.MicrosoftLogsPath.FullPath;
        var internalLoggingSource = new LoggingSource(LogMode.None, logPath, "MicrosoftLoggingSource", TimeSpan.MaxValue, long.MaxValue);

        var maxFileSize = configuration.MicrosoftLogsMaxFileSize ?? configuration.MaxFileSize;
        var retentionTime = configuration.MicrosoftLogsRetentionTime ?? configuration.RetentionTime;
        var retentionSize = configuration.MicrosoftLogsRetentionSize ?? configuration.RetentionSize;
        var compress = configuration.MicrosoftLogsCompress ?? configuration.Compress;
        
        internalLoggingSource.MaxFileSizeInBytes = maxFileSize.GetValue(SizeUnit.Bytes);
        internalLoggingSource.SetupLogMode(LogMode.Information, logPath, retentionTime?.AsTimeSpan, retentionSize?.GetValue(SizeUnit.Bytes), compress);
        
        loggingSource =  internalLoggingSource;
        return hostBuilder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
            var configurationBuilder = new ConfigurationBuilder();
            void ConfigureConfigurationSource(JsonConfigurationSource source)
            {
                string jsonPath = configuration.MicrosoftLogsConfigurationPath.FullPath;
                source.Path = jsonPath;
                source.Optional = true;
                source.ReloadOnChange = true;
                source.ResolveFileProvider();
                source.OnLoadException = context =>
                {
                    //Soft exception handling
                    context.Ignore = true;
                    
                    var alert = AlertRaised.Create(
                        null,
                        "Microsoft Logs Configuration Load Failed",
                        $"Failed to load microsoft logs configuration from json file {jsonPath}",
                        AlertType.MicrosoftLogsConfigurationLoadError,
                        NotificationSeverity.Warning,
                        key: "microsoft-configuration-logs-error",
                        details: new ExceptionDetails(context.Exception));
                    notificationCenter.Add(alert);
                    
                    if (Logger.IsInfoEnabled) 
                        Logger.Info($"Unable to load Microsoft Logs configuration", context.Exception);
                };
            }

            configurationBuilder.AddJsonFile(ConfigureConfigurationSource);
            logging.AddConfiguration(configurationBuilder.Build());
            logging.SetMinimumLevel(LogLevel.Critical);
        
            logging.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SparrowLoggingProvider>(serviceProvider => new SparrowLoggingProvider(internalLoggingSource)));
        });
    }
}


