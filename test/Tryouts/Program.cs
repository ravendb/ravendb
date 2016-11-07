using System;
using System.Diagnostics;
using FastTests.Issues;
using FastTests.Sparrow;
using FastTests.Voron.Bugs;

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
                using (var a = new SlowTests.Core.Indexing.ReferencedDocuments())
                {
                    a.BasicLoadDocuments_Casing();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

