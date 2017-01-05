using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Voron;
using SlowTests.Voron;
using StressTests;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            //Parallel.For(0, 100, i =>
            //{
            //    Console.WriteLine(i);
            //    using (var a = new SlowTests.Tests.Sorting.AlphaNumericSorting())
            //    {
            //        a.random_words_using_document_query_async().Wait();
            //    }
            //});

            for (int i = 0; i < 199; i++)
            {
                var sp = Stopwatch.StartNew();
                using (var a = new LongKeys())
                {
                    a.NoDebugAssertShouldThrownDuringRebalancing(seed: 4);
                }
                Console.WriteLine(sp.ElapsedMilliseconds);
            }
        }
    }


}

