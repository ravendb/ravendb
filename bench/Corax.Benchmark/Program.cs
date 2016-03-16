using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron;

namespace Corax.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
#if DEBUG
            var oldfg = Console.ForegroundColor;
            var oldbg = Console.BackgroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Yellow;

            Console.WriteLine("Dude, you are running benchmark in debug mode?!");
            Console.ForegroundColor = oldfg;
            Console.BackgroundColor = oldbg;
#endif

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.ForPath(Configuration.Path), new DefaultAnalyzer()))
            {
                var indexer = _fullTextIndex.CreateIndexer();

                foreach (var doc in loader.LoadAsDocuments())
                {
                    indexer.NewEntry(doc.Item2, doc.Item1);
                }
            }
        }
    }
}
