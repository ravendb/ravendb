using System;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Raven.Server.Config.Categories;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class WebHostBuilderExtensions
{
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, ref LoggingSource loggingSource, LogsConfiguration configurationLogs)
    {
        if (configurationLogs.DisableMicrosoftLogs)
            return hostBuilder;
        
        loggingSource = new LoggingSource(LogMode.Information, configurationLogs.MicrosoftLogPath.FullPath, "Microsoft Log", TimeSpan.MaxValue, long.MaxValue);
        
        var internalLoggingSource = loggingSource;
        return hostBuilder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddJsonFile(null, configurationLogs.MicrosoftLogConfigurationPath.FullPath, true, true);
            logging.AddConfiguration(configurationBuilder.Build());
            logging.SetMinimumLevel(LogLevel.Critical);
        
            logging.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SparrowLoggingProvider>(serviceProvider => new SparrowLoggingProvider(internalLoggingSource)));
        });
    }
}


