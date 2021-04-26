using System;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.Utils
{
    public class UnhandledExceptions
    {
        internal static readonly TimeSpan TimeToWaitForLog = TimeSpan.FromSeconds(15);

        public static void Track(Logger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
            {
                if (logger.IsOperationsEnabled == false)
                    return;
                Task task;
                if (args.ExceptionObject is Exception ex)
                {
                    task = logger.OperationsAsync("UnhandledException occurred.", ex);
                }
                else
                {
                    var exceptionString = $"UnhandledException: { args.ExceptionObject?.ToString()  ?? "null" }.";
                    task = logger.OperationsAsync(exceptionString);
                }

                Console.Error.WriteLine("UnhandledException occured");
                Console.Error.WriteLine(args.ExceptionObject);


                task.Wait(TimeToWaitForLog);
            };

            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                if (logger.IsInfoEnabled == false)
                    return;

                if (args.Observed)
                    return;

                logger.InfoAsync("UnobservedTaskException occurred.", args.Exception).Wait(TimeToWaitForLog);
            };
        }
    }
}
