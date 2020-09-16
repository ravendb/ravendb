using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Client;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
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
            for (int i = 0; i < 123; i++)
            {
                var sp = Stopwatch.StartNew();
                Console.WriteLine($"Starting to run {i}");

                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new RavenD_15608(testOutputHelper))
                    {
                        await test.ArtificalDocumentsMatchIndexEntries();
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                    // Console.ReadLine();
                }
                finally
                {
                    Console.WriteLine($"test ran for {sp.ElapsedMilliseconds:#,#}ms");
                }
            }
        }
    }
}
