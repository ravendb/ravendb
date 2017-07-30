using System;
using SlowTests.Server.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Program", "Program");

        public static void Main(string[] args)
        {
            //   LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\ravendb\logs");

            for (int i = 0; i < 100000; i++)
            {
                Console.WriteLine(i);
                Logger.Info("Program: " + i);

                using (var test = new SlowTests.Server.Replication.ReplicationSpecialCases())
                {
                    try
                    {
                        test.NonIdenticalContentConflict().Wait();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                        Console.Beep();
                        Console.Beep();
                        return;
                    }
                }
            }
        }
    }
}
