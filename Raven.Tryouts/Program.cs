using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Impl;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Faceted;

namespace Raven.Tryouts
{
    class Program
    {
        static void Main()
        {
            //var temp = new StablePaging();
            //temp.CanPageDespiteDeletingDocs();

            //MemoryTest();

            var temp = new FacetedIndex();
            temp.CanPerformFacetedSearch();
        }

        private static void MemoryTest()
        {
            //IOExtensions.DeleteDirectory("Data");
            using (var documentStore = new EmbeddableDocumentStore())
            {
                documentStore.Configuration.DataDirectory = "Data";
                documentStore.Configuration.DefaultStorageTypeName = "esent";
                documentStore.Configuration.Settings["Raven/Esent/CacheSizeMax"] = "512";
                documentStore.Configuration.Settings["Raven/Esent/MaxVerPages"] = "10";
                documentStore.Configuration.Settings["Raven/MemoryCacheLimitPercentage"] = "10";
                documentStore.Initialize();
                var index = new RavenDocumentsByEntityName();
                index.Execute(documentStore);
                var sw = Stopwatch.StartNew();

                var data = new String('x', 20480);
                var list = new List<decimal>(Enumerable.Range(0, 10000).Select(x => (decimal)x));

                // Insert some setup data
                using (var session = documentStore.OpenSession())
                {
                    var testFoo = new Foo
                                      {
                                          Data = data,
                                          List = list,
                                          Counter = 0
                                      };
                    var bytes = RavenJObject.FromObject(testFoo).ToBytes();
                    //json = RavenJObject.FromObject(testFoo).ToString();                    
                    Console.WriteLine("Doc as BinaryJson is {0} bytes ({1:0.00}K or {2:0.00} MB)",
                                      bytes.Length, bytes.Length / 1024.0, bytes.Length / 1024.0 / 1024.0);
                    session.Store(testFoo);
                    session.SaveChanges();

                    //var highestId = session.Query<Foo>()
                    //                    .Customize(x => x.WaitForNonStaleResults())
                    //                    .OrderByDescending(x => x.Id)                                        
                    //                    .FirstOrDefault();
                    //Console.WriteLine("Highest Id: " + highestId.Id);
                }                

                using (var textLog = new StreamWriter("log.txt"))
                using (var csvlog = new StreamWriter("log.csv"))
                {
                    csvlog.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}, {6}",
                                     "Elapsed", "Counter", "Private Mem (MB)", "Total .NET Mem (MB)", 
                                     "Doc store size (MB)", "Insert Time (ms)", "Query Time (ms)");
                    var counter = 0;
                    while (!Console.KeyAvailable)
                    {                        
                        var foo = new Foo
                                      {
                                          Data = data,
                                          List = list,
                                          Counter = ++counter
                                      };

                        // Insert
                        Stopwatch insertTimer = Stopwatch.StartNew();
                        using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
                        {                            
                            using (var session = documentStore.OpenSession())
                            {
                                session.Store(foo);
                                session.SaveChanges();
                            }
                        }
                        insertTimer.Stop();

                        // Query
                        Stopwatch queryTimer = Stopwatch.StartNew();
                        int subQueryCount = 0;
                        //using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
                        {                            
                            using (var session = documentStore.OpenSession())
                            {
                                try
                                {
                                    var counter1 = counter;
                                    RavenQueryStatistics stats;                                    
                                    do
                                    {
                                        var subQueryTime = Stopwatch.StartNew();
                                        var firstOrDefault = session.Query<Foo>()
                                            //.Customize(x => x.WaitForNonStaleResults())
                                            .Statistics(out stats)
                                            .Where(x => x.Counter == counter1)
                                            .FirstOrDefault();
                                        subQueryTime.Stop();
                                        //Console.WriteLine("Sub-query took {0:0.00} ms", queryTime.ElapsedMilliseconds);
                                        Debug.Assert(firstOrDefault != null);
                                        Debug.Assert(firstOrDefault.Counter == counter1);
                                        Thread.Sleep(100);
                                        subQueryCount++;
                                    } while (stats.IsStale);
                                }
                                catch (TimeoutException tEx)
                                {
                                    Console.WriteLine(tEx.Message);
                                }                                
                            }
                        }
                        queryTimer.Stop();

                        // Update
                        //using (var session = documentStore.OpenSession())
                        //{
                        //    foo.Data = new String('y', 2048);
                        //    session.Store(foo);
                        //    session.SaveChanges();
                        //}

                        double docStoreSize =
                            documentStore.DocumentDatabase.TransactionalStorage.GetDatabaseSizeInBytes();
                        docStoreSize = docStoreSize / 1024.0 / 1024.0;

                        var gcTimer = Stopwatch.StartNew();
                        var gcSize = GC.GetTotalMemory(true)/1024.0/1024.0;
                        gcTimer.Stop();

                        var statsTimer = Stopwatch.StartNew();
                        var memoryStats =
                            String.Format("{0} {1} {2} private {3:0.00} MB, .NET managed {4:0.00} MB, store {5:0.00} MB",
                                          DateTime.Now.ToLongTimeString(), sw.ElapsedMilliseconds, counter,
                                          Process.GetCurrentProcess().PrivateMemorySize64/1024.0/1024.0,
                                          gcSize, docStoreSize);
                        var docDbStats = documentStore.DocumentDatabase.Statistics;              
                        var timingStats = String.Format("        {0}, insert took {1} ms, query {2} ms ({3} sub-queries), gc {4} ms",
                                                        counter, insertTimer.ElapsedMilliseconds,
                                                        queryTimer.ElapsedMilliseconds,
                                                        subQueryCount,
                                                        gcTimer.ElapsedMilliseconds);
                        Console.WriteLine(memoryStats);
                        Console.WriteLine(timingStats);                        
                        textLog.WriteLine(memoryStats);
                        textLog.WriteLine(timingStats);
                        textLog.Flush();
                        csvlog.WriteLine("{0}, {1}, {2:0.00}, {3:0.00}, {4:0.00}, {5:0.00}, {6:0.00}, {7}",
                                         sw.Elapsed, counter,
                                         Process.GetCurrentProcess().PrivateMemorySize64/1024.0/1024.0,
                                         GC.GetTotalMemory(false)/1024.0/1024.0,
                                         docStoreSize,
                                         insertTimer.ElapsedMilliseconds,
                                         queryTimer.ElapsedMilliseconds,
                                         subQueryCount);
                        csvlog.Flush();
                        statsTimer.Stop();
                        Console.WriteLine("Took {0} ms to collect and log stats", statsTimer.ElapsedMilliseconds);
                    }
                }
            }
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Data { get; set; }
        public List<decimal> List { get; set; }
        public int Counter { get; set; }
    }
}
