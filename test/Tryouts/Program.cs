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
using System.Security.Cryptography.X509Certificates;
using FastTests.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using SlowTests.Voron.Issues;
using Voron;
using Voron.Data.Tables;
using Sparrow.Binary;
using Voron.Data.Fixed;
using SlowTests.Authentication;
using FastTests.Sparrow;
using SlowTests.Server.NotificationCenter;
using FastTests;
using Xunit;
using Raven.Client.Documents.Commands.Batches;
using Sparrow.Json;
using Raven.Client.Documents.Operations;
using SlowTests.Issues;

namespace Tryouts
{
    public class BlittableTest : RavenTestBase
    {
        [Fact]
        public void TestDecimalNumbers()
        {
            using (var store = GetDocumentStore())
            {
                                
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {

                    for (var i = 0; i < 1000; i++)
                    {
                        var blittable = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                        {
                            ["Number"] = Decimal.MaxValue - (decimal)i  - (decimal)0.1
                            //["Number"] = double.MaxValue - (double)i - (double)0.1

                        }, "someDoc");

                        store.Commands().Put($"users/{i}", null, blittable,new Dictionary<string, object>
                        {
                            { Raven.Client.Constants.Documents.Metadata.Collection, "MyUsesrs" }
                        });
                    }

                 //   var operation = store.Operations.Send(new PatchByQueryOperation("from MyUsesrs as u update { u.Number -= 1;}"));

                 

                    Thread.Sleep(10);
                    //where u.Number < {Decimal.MaxValue - 500}
                    var r = store.Commands().Query(new Raven.Client.Documents.Queries.IndexQuery()
                    {
                        Query = $@"from MyUsesrs as u

order by u.Number as double
select {{Number:u.Number}}
"
                    });

                    var results = r.Results.ToList();

                    var user = store.Commands().Get("users/1");
                    Console.WriteLine(user["Number"]);                    
                }
                

                
            }
        }
    }
    class Program
    {
        private const int DocumentsCount = 1000;

        public class User
        {
            public string Name { get; set; }
        }

        public static async Task Main(string[] args)
        {
            new RavenDB_10537().TestDecimalNumbers();
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
