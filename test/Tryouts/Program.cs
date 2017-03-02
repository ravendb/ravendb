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
            for (int i = 0; i < 100; i++)
            {
                
                //LoggingSource.Instance.SetupLogMode(LogMode.Information, "logs");
                //LoggingSource.Instance.EnableConsoleLogging();
                var sp = Stopwatch.StartNew();
                using (var a = new SlowTests.Server.Rachis.BasicCluster())
                {
                    a.ClusterWithThreeNodesAndElections().Wait();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }
}