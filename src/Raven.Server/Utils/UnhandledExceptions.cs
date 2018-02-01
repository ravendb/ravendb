using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.Utils
{
    public class UnhandledExceptions
    {
        public static void Track(Logger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
            {
                var exceptionString = $"UnhandledException: { eventArgs.ExceptionObject.ToString() }.";
                if (logger.IsInfoEnabled)
                    logger.Info(exceptionString);
            };

            TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
            {
                var exception = eventArgs.Exception.ToString();
                if (logger.IsInfoEnabled)
                    logger.Info($"UnobservedTaskException: {exception}.");
            };
        }
    }
}
