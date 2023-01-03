using System;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class WebHostBuilderExtensions
{
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, ref MicrosoftLoggingProvider sparrowLoggingProvider, LogsConfiguration configuration,
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
        
        sparrowLoggingProvider =  new MicrosoftLoggingProvider(internalLoggingSource, notificationCenter);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            sparrowLoggingProvider.InitAsync(configuration.MicrosoftLogsConfigurationPath.FullPath, context).GetAwaiter().GetResult();
        }
        var internalSparrowLoggingProvider = sparrowLoggingProvider;
        return hostBuilder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
            //We control the log availability ourself
            logging.SetMinimumLevel(LogLevel.Trace);

            logging.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, MicrosoftLoggingProvider>(serviceProvider => internalSparrowLoggingProvider));
        });
    }
}


