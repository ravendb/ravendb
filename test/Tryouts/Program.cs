using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                var sp = Stopwatch.StartNew();
                using (var x = new FastTests.Server.Documents.Indexing.Auto.BasicAutoMapIndexing())
                {
                    x.IndexLoadErrorCreatesFaultyInMemoryIndexFakeAndAddsAlert();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

