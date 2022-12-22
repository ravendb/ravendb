using System;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class WebHostBuilderExtensions
{
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, ref LoggingSource loggingSource, LogsConfiguration configurationLogs)
    {
        if (configurationLogs.DisableMicrosoftLogs)
            return hostBuilder;
        
        string logPath = configurationLogs.MicrosoftLogsPath.FullPath;
        var internalLoggingSource = new LoggingSource(LogMode.None, logPath, "Microsoft Log", TimeSpan.MaxValue, long.MaxValue);
        internalLoggingSource.MaxFileSizeInBytes = configurationLogs.MicrosoftLogsMaxFileSize.GetValue(SizeUnit.Bytes);
        internalLoggingSource.SetupLogMode(
            LogMode.Information, 
            logPath, 
            configurationLogs.MicrosoftLogsRetentionTime?.AsTimeSpan, 
            configurationLogs.MicrosoftLogsRetentionSize?.GetValue(SizeUnit.Bytes), 
            configurationLogs.MicrosoftLogsCompress);
        
        loggingSource =  internalLoggingSource;
        return hostBuilder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddJsonFile(null, configurationLogs.MicrosoftLogsConfigurationPath.FullPath, true, true);
            logging.AddConfiguration(configurationBuilder.Build());
            logging.SetMinimumLevel(LogLevel.Critical);
        
            logging.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SparrowLoggingProvider>(serviceProvider => new SparrowLoggingProvider(internalLoggingSource)));
        });
    }
}


