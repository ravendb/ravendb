using System;
using System.Diagnostics;
using System.Threading.Tasks;
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
            Parallel.For(0, 10, i =>
            {
                var sp = Stopwatch.StartNew();
                using (var a = new SlowTests.Server.Rachis.BasicCluster())
                {
                    a.ClusterWithThreeNodesAndElections().Wait();
                }
                Console.WriteLine(sp.Elapsed);
            });
          
        }
    }
}