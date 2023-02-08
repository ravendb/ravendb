using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class WebHostBuilderExtensions
{
    public static IWebHostBuilder ConfigureMicrosoftLogging(this IWebHostBuilder hostBuilder, LogsConfiguration configuration,
        ServerNotificationCenter notificationCenter)
    {
        //Because we use our own implementation of ILoggerFactory we can just define the factory as service
        //But to keep similar to documentation instructions, `ConfigureLogging` is used
        return hostBuilder.ConfigureLogging(logging =>
        {
            var loggingProvider = new MicrosoftLoggingProvider(LoggingSource.Instance, notificationCenter);
            using(var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationPath = configuration.MicrosoftLogsConfigurationPath.FullPath;
                loggingProvider.Configuration.ReadConfiguration(configurationPath, context, false);
                if(configuration.DisableMicrosoftLogs == false)
                    loggingProvider.ApplyConfiguration();
            }

            logging.Services.AddSingleton(_ => loggingProvider);
            logging.Services.AddSingleton<ILoggerFactory, MicrosoftLoggerFactory>(_ => new MicrosoftLoggerFactory(loggingProvider));
        });
    }
}


