using System;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.Utils
{
    public class UnhandledExceptions
    {
        private static readonly TimeSpan TimeToWaitForLog = TimeSpan.FromSeconds(15);

        public static void Track(Logger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
            {
                var exceptionString = $"UnhandledException: { eventArgs.ExceptionObject.ToString() }.";
                if (logger.IsOperationsEnabled)
                    logger.Operations(exceptionString, withWait: true).Wait(TimeToWaitForLog);
            };

            TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
            {
                var exception = eventArgs.Exception.ToString();
                if (logger.IsInfoEnabled)
                    logger.Info($"UnobservedTaskException: {exception}.", withWait: true).Wait(TimeToWaitForLog);
            };
        }
    }
}
