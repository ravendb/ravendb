using System;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow.Logging;

namespace Raven.Server.Utils
{
    public class UnhandledExceptions
    {
        private static readonly TimeSpan TimeToWaitForLog = TimeSpan.FromSeconds(15);

        public static void Track(Logger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
            {
                if (logger.IsOperationsEnabled == false)
                    return;

                if (args.ExceptionObject is Exception ex)
                {
                    logger.OperationsAsync("UnhandledException occurred.", ex).Wait(TimeToWaitForLog);
                }
                else
                {
                    var exceptionString = $"UnhandledException: { args.ExceptionObject.ToString() }.";
                    logger.OperationsAsync(exceptionString).Wait(TimeToWaitForLog);
                }
            };

            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                if (logger.IsInfoEnabled == false)
                    return;

                if (args.Observed)
                    return;

                var exception = args.Exception.ExtractSingleInnerException();
                logger.InfoAsync("UnobservedTaskException occurred.", exception).Wait(TimeToWaitForLog);
            };
        }
    }
}
