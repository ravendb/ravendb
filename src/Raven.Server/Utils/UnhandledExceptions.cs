using System;
using System.Threading.Tasks;
using NLog;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils
{
    public sealed class UnhandledExceptions
    {
        public static void Track(RavenLogger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
            {
                if (logger.IsFatalEnabled == false)
                    return;
                if (args.ExceptionObject is Exception ex)
                {
                    logger.Fatal("UnhandledException occurred.", ex);
                }
                else
                {
                    var exceptionString = $"UnhandledException: { args.ExceptionObject?.ToString()  ?? "null" }.";
                    logger.Fatal(exceptionString);
                }

                Console.Error.WriteLine("UnhandledException occurred");
                Console.Error.WriteLine(args.ExceptionObject);

                LogManager.Shutdown();
            };

            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                if (logger.IsInfoEnabled == false)
                    return;

                if (args.Observed)
                    return;

                logger.Info("UnobservedTaskException occurred.", args.Exception);
            };
        }
    }
}
