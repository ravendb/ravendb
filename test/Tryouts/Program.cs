using Raven.Client.Documents;
using RachisTests.DatabaseCluster;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Utils;
using SlowTests.Server.Replication;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide;
using System.Threading;
using System.Net;

/*
    Code reference - please DO NOT REMOVE:
         
    DebuggerAttachedTimeout.DisableLongTimespan = true;
    
    Console.WriteLine(Process.GetCurrentProcess().Id);
    Console.WriteLine();
    
    LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\ravendb\logs");
 */

namespace Tryouts
{
    class Program
    {
        private const int DocumentsCount = 1000;

        public class User
        {
            public string Name { get; set; }
        }
        static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 100;
            ThreadPool.SetMinThreads(25, 25);
            for (int i = 0; i < 100; i++)
            {
                //ResetBench();
                ParallelBatchStores(1);
                SimpleMap100QueriesParallelAllResults(20);                
            }
        }

        private static void ResetBench()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" }
            }.Initialize())
            {
                try
                {
                    var res = store.Maintenance.Server.Send(new DeleteDatabasesOperation("Bench", true));                    
                }
                catch { }

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord
                {
                    DatabaseName = "Bench"
                }));
            }
        }

        private static void SerialStores()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                for (int i = 0; i < DocumentsCount; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = i.ToString() });
                        session.SaveChanges();
                    }
                }

                Console.WriteLine($"Serial stores: {sp.ElapsedMilliseconds}");
            }
        }


        private static void ParallelStores(int multiplier)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                Parallel.For(0, DocumentsCount, new ParallelOptions
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount * multiplier
                }, i =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = i.ToString() });
                        session.SaveChanges();
                    }
                });

                Console.WriteLine($"Parallel stores (x{multiplier}): {sp.ElapsedMilliseconds}");
            }
        }

        private static void SerialBatchStores()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (var j = 0; j < DocumentsCount / 10; j++)
                            session.Store(new User { Name = i.ToString() });
                        session.SaveChanges();
                    }
                }

                Console.WriteLine($"Serial batches stores: {sp.ElapsedMilliseconds}");
            }
        }

        private static void ParallelBatchStores(int multiplier)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                Parallel.For(0, 10, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * multiplier }, i =>
                {
                    using (var session = store.OpenSession())
                    {
                        for (var j = 0; j < DocumentsCount / 10; j++)
                            session.Store(new User { Name = i.ToString() });
                        session.SaveChanges();
                    }
                });

                Console.WriteLine($"Parallel batches stores (x{multiplier}): {sp.ElapsedMilliseconds}");
            }
        }

        private static void BulkInsert()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                using (var bi = store.BulkInsert())
                {
                    for (var i = 0; i < DocumentsCount; i++)
                    {
                        bi.Store(new User { Name = i.ToString() });
                    }
                };

                Console.WriteLine($"BulkInsert: {sp.ElapsedMilliseconds}");
            }
        }

        private static void ParallelBulkInserts(int multiplier)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                Parallel.For(0, 10, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * multiplier }, i =>
                {
                    using (var session = store.BulkInsert())
                    {
                        for (var j = 0; j < DocumentsCount / 10; j++)
                            session.Store(new User { Name = i.ToString() });
                    }
                });

                Console.WriteLine($"Parallel bulk inserts (x{multiplier}): {sp.ElapsedMilliseconds}");
            }
        }

        private static void SimpleMapIndexing()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    var sp = Stopwatch.StartNew();
                    session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "5").ToList();
                    Console.WriteLine($"Simple map index, single result: {sp.ElapsedMilliseconds}");
                }
            }
        }

        private static void SimpleMapIndexingAllResults()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    var sp = Stopwatch.StartNew();
                    session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name != "a")
                        .ToList();
                    Console.WriteLine($"Simple map index, all results: {sp.ElapsedMilliseconds}");
                }
            }
        }

        private static void MapReduceIndexingAllResults()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    var sp = Stopwatch.StartNew();
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Name, x => 1, (keyName, g) => new
                        {
                            Name = keyName,
                            Amount = g.Sum()
                        }).ToList();
                    Console.WriteLine($"Map reduce indexing, all results: {sp.ElapsedMilliseconds}");
                }
            }
        }

        private static void SimpleMap100QueriesAllResults()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {

                        session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name != "a").ToList();

                    }
                }

                Console.WriteLine($"Simple map index, 100 queries all results: {sp.ElapsedMilliseconds}");
            }
        }

        private static void SimpleMap100QueriesParallelAllResults(int modifier)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost.fiddler:8080" },
                Database = "Bench"
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                var completed = Parallel.For(0, 1000,
                    new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * modifier },
                    i =>
                    {
                        using (var session = store.OpenSession())
                        {

                            session.Query<User>().Customize(x => x.WaitForNonStaleResults())
                                .Where(x => x.Name != "a")
                                .ToList();

                        }
                    }).IsCompleted;

                Console.WriteLine($"Simple map index, 100 queries parallel (x{modifier})all results: {sp.ElapsedMilliseconds}, completed: {completed}");
            }
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public Dictionary<string, double> NumVals { get; set; }
    }
}
