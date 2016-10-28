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
                using (var x = new SlowTests.Tests.Sorting.AlphaNumericSorting())
                {
                    x.random_words_using_document_query_async().Wait();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

