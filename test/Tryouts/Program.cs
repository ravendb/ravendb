using System;
using System.Diagnostics;
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
            {

                var sp = Stopwatch.StartNew();
                using (var a = new SlowTests.SlowTests.Faceted.FacetPaging())
                {
                    a.CanPerformFacetedPagingSearchWithPageSize_TermDesc_LuceneQuery();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }
}