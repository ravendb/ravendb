using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            var threads = new List<Thread>();
            var loggerSetup = new Sparrow.Logging.LoggerSetup("logs");

            for (int i = 0; i < 16; i++)
            {
                var msg = "test ";
                for (int j = 0; j < i; j++)
                {
                    msg += " test ";
                }
                threads.Add(new Thread(() =>
                {
                    var logger = loggerSetup.GetLogger<Program>("Northwind");
                    for (int j = 0; j < 10 * 1000 * 1000; j++)
                    {
                        logger.Info(msg);
                    }
                })
                {
                    IsBackground = true,
                    Name = "Worker"
                });
            }
            var sp = Stopwatch.StartNew();
            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }
            Console.WriteLine(sp.Elapsed);
        }
    }
}