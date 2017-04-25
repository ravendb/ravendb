using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.PeriodicExport;
using FastTests.Server.OAuth;
using FastTests.Server.Replication;
using SlowTests.Issues;
using Sparrow;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                Parallel.For(0, 1, j =>
                {
                    using (var a = new SlowTests.Server.Rachis.IndexesAndTransformers())
                    {
                        a.BasicTransformerCreation().Wait();
                    }
                });
            }
        }
    }
}