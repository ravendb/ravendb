using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using StressTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 123; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var test = new RavenDB_13796())
                    {
                        await test.TopologyUpdateDuringFailoverShouldntFaileCommand();
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
