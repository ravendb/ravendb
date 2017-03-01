using System;
using System.Diagnostics;
using SlowTests.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();
            {
                //LoggingSource.Instance.SetupLogMode(LogMode.Information, "logs");
                //LoggingSource.Instance.EnableConsoleLogging();
                var sp = Stopwatch.StartNew();
                using (var a = new FastTests.Client.Lazy.Async.lazyAsync())
                {
                    a.CanExecuteAllPendingLazyOperations().Wait();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }
}