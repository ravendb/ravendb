using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Corax;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            //CoraxEnron.IndexInCorax(true);
            //LuceneEnron.IndexInLucene(true);

            //CoraxReddit.Index(true, "Z:\\corax");
            //LuceneReddit.Index(true, "Z:\\corax");
            CoraxReddit.SearchExact("Z:\\corax");
            //LuceneReddit.SearchExact("Z:\\corax");

            //using (var searcher = new IndexSearcher(env))
            //{
            //    QueryOp q = new BinaryQuery(
            //        new QueryOp[] {new TermQuery("Dogs", "Arava"), new TermQuery("Gender", "Male"),},
            //        BitmapOp.And
            //    );
            //    using var ctx = JsonOperationContext.ShortTermSingleUse();
            //    var results = searcher.Query(ctx, q, 2, "Name");

            //    foreach (object result in results)
            //    {
            //        Console.WriteLine(result);
            //    }
            //}

            Console.WriteLine("Done");
            Console.ReadLine();
        }
    }
}
