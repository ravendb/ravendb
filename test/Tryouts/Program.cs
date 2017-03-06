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

            using (var a = new FastTests.Sparrow.IoMetric())
            {
                a.CanReportMetricsInParallel();
            }
        }
    }
}