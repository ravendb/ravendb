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
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, ref LoggingSource loggingSource, LogsConfiguration configuration)
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
            configurationBuilder.AddJsonFile(null, configuration.MicrosoftLogsConfigurationPath.FullPath, true, true);
            logging.AddConfiguration(configurationBuilder.Build());
            logging.SetMinimumLevel(LogLevel.Critical);
        
            logging.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SparrowLoggingProvider>(serviceProvider => new SparrowLoggingProvider(internalLoggingSource)));
        });
    }
}


