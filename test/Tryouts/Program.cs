using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            //Parallel.For(0, 100, i =>
            {
                using (var a = new FastTests.Server.Documents.PeriodicExport.PeriodicExportTests())
                {
                    a.CanSetupPeriodicExportWithVeryLargePeriods().Wait();
                }
            }
        }
    }
}