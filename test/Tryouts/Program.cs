using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.Benchmark;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Indexing;
using FastTests.Voron.Bugs;
using Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Tryouts.Corax;
using Tryouts.Corax.Tests;
using Voron;
using Voron.Debugging;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            GC.KeepAlive(typeof(DocumentStore));
            //WriteToStreamBenchmark.PerformanceAnalysis(@"d:\json\big", Path.GetTempFileName(), 2);
            //////Console.WriteLine("Ready...");
            //////Console.WriteLine();
            //////Console.ReadLine();
            //WriteToStreamBenchmark.PerformanceAnalysis(@"d:\json\big", Path.GetTempFileName(), int.MaxValue);
            ////WriteToStreamBenchmark.ManySmallDocs(@"d:\json\lines", 1);
            ////Console.WriteLine();
            ////WriteToStreamBenchmark.ManySmallDocs(@"d:\json\lines", int.MaxValue);
            var basicIndexUsage = new FullTextSearch();
            basicIndexUsage.CanSort();

            //Run();
        }

        private static void Run()
        {

            ForceInit();

            var sp = Stopwatch.StartNew();

            CheckIndexer();

            Console.WriteLine(sp.ElapsedMilliseconds);

        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void CheckIndexer()
        {
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("test55");
            storageEnvironmentOptions.ManualFlushing = true;
            using (var corax = new FullTextIndex(storageEnvironmentOptions, new DefaultAnalyzer()))
            {
                for (int a = 0; a < 10; a++)
                {
                    using (var indexer = corax.CreateIndexer())
                    {
                        int index = 0;
                        {
                            foreach (var line in File.ReadLines(@"C:\Users\Ayende\Downloads\pr.txt"))
                            {
                                indexer.NewEntry(new DynamicJsonValue
                                {
                                    ["Location"] = line,
                                    ["Active"] = "true",
                                    ["Age"] = (index % 120).ToString(),
                                    ["Name"] = line.Substring(0, Math.Min(15, line.Length))
                                }, "users/" + (++index));
                            }
                        }
                    }
                    corax.Env.FlushLogToDataFile();

                    var environmentStats = corax.Env.Stats();
                    Console.WriteLine(JsonConvert.SerializeObject(environmentStats, Formatting.Indented));

                }


                //using (var searcher = corax.CreateSearcher())
                //{
                //    var ids = searcher.Query("Name", "Oren Eini");
                //    Console.WriteLine(ids.Length);
                //    //Assert.Equal(new[] { "users/1" }, ids);
                //}

                ////using (var indexer = corax.CreateIndexer())
                ////{
                ////   indexer.Delete("users/1");
                ////}

                //using (var searcher = corax.CreateSearcher())
                //{
                //    var ids = searcher.Query("Name", "Oren Eini");
                //    Assert.Empty(ids);
                //}
            }
        }

        private static void ForceInit()
        {
            var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
            storageEnvironmentOptions.ManualFlushing = true;
            using (var corax = new FullTextIndex(storageEnvironmentOptions, new DefaultAnalyzer()))
            {
                using (var indexer = corax.CreateIndexer())
                {
                    for (int a = 0; a < 1; a++)
                    {
                        int index = 0;
                        foreach (var line in File.ReadLines(@"C:\Users\Ayende\Downloads\places.txt"))
                        {
                            indexer.NewEntry(new DynamicJsonValue
                            {
                                ["Location"] = line
                            }, "users/" + (++index));

                        }
                    }
                }
            }
        }
    }
}
