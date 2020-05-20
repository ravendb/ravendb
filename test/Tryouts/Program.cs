using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server.Documents.ETL;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Voron;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }
        
        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 1; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new EtlTimeSeriesTests(testOutputHelper))
                    {
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                   // Console.ReadLine();
                }
            }
        }
    }
}
