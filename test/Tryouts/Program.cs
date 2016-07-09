using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine( i);
                using (var n = new FastTests.Server.Documents.Indexing.Auto.BasicAutoMapIndexing())
                {
                    n.WriteErrors();
                }
            }
        }
    }
}