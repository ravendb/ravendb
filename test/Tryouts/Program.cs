using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Blittable;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using FastTests.Voron.Bugs;
using Newtonsoft.Json;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Tryouts.Corax;
using Voron;
using Voron.Debugging;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //new DuplicatePageUsage().ShouldNotHappen();
            //new MetricsTests().MetricsTest();
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
            var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
            storageEnvironmentOptions.ManualFlushing = true;
            using (var corax = new FullTextIndex(storageEnvironmentOptions))
            {
                for (int a = 0; a < 50; a++)
                {
                    using (var indexer = corax.CreateIndexer())
                    {
                        int index = 0;
                        foreach (var line in File.ReadLines(@"C:\Users\Ayende\Downloads\pr.txt"))
                        {
                            indexer.NewEntry(new DynamicJsonValue
                            {
                                ["Location"] = line,
                                ["Active"] = "true",
                                ["Age"] = (index % 120).ToString(),
                                ["Name"] = line.Substring(0, Math.Min(15, line.Length))
                            }, "users/" + (++index)).Wait();
                        }
                    }
                }

                //corax.Environment.FlushLogToDataFile();

                //var environmentStats = corax.Environment.Stats();
                //Console.WriteLine(JsonConvert.SerializeObject(environmentStats,Formatting.Indented));

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
            using (var corax = new FullTextIndex(storageEnvironmentOptions))
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
                            }, "users/" + (++index)).Wait();

                        }
                    }
                }
            }
        }
    }
}
