using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Identity;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable CS0618

namespace FastTests.Client
{
    public class Hilo : ReplicationTestBase
    {
        public Hilo(ITestOutputHelper output) : base(output)
        {
        }

        private class HiloDoc
        {
            public long Max { get; set; }
        }

        private class Product
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Hilo_Cannot_Go_Down(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var hiloDoc = new HiloDoc
                    {
                        Max = 32
                    };
                    session.Store(hiloDoc, "Raven/Hilo/users");
                    session.SaveChanges();

                    var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store, store.Database,
                        store.Conventions.IdentityPartsSeparator);

                    var ids = new HashSet<long> { await GetNextIdAsync(hiLoKeyGenerator) };

                    hiloDoc.Max = 12;
                    session.Store(hiloDoc, null, "Raven/Hilo/users");
                    session.SaveChanges();

                    for (int i = 0; i < 128; i++)
                    {
                        var nextId = await GetNextIdAsync(hiLoKeyGenerator);
                        Assert.True(ids.Add(nextId), "Failed at " + i);
                    }

                    var list = ids.GroupBy(x => x).Select(g => new
                    {
                        g.Key,
                        Count = g.Count()
                    }).Where(x => x.Count > 1).ToList();

                    Assert.Empty(list);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLo_Async_MultiDb(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, "Raven/Hilo/users");

                    session.Store(new HiloDoc
                    {
                        Max = 128
                    }, "Raven/Hilo/products");

                    session.SaveChanges();


                    var multiDbHiLo = new AsyncMultiDatabaseHiLoIdGenerator(store);

                    var generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(null, new User());
                    Assert.Equal("users/65-A", generateDocumentKey);

                    generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(null, new Product());
                    Assert.Equal("products/129-A", generateDocumentKey);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Generate_HiLo_Ids(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var multiDbHiLo = new AsyncMultiDatabaseHiLoIdGenerator(store);

                var usersIds = new ConcurrentSet<long>();
                var productsIds = new ConcurrentSet<long>();
                var count = 10;

                await Parallel.ForEachAsync(Enumerable.Range(0, count), async (_, _) =>
                {
                    var id = await multiDbHiLo.GenerateNextIdForAsync(null, "Users");
                    Assert.True(usersIds.TryAdd(id));

                    id = await multiDbHiLo.GenerateNextIdForAsync(null, "Products");
                    Assert.True(productsIds.TryAdd(id));
                });

                Assert.Equal(count, usersIds.Count);
                Assert.Equal(count, productsIds.Count);

                await Parallel.ForEachAsync(Enumerable.Range(0, count), async (_, _) =>
                {
                    var id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, "Users");
                    Assert.True(usersIds.TryAdd(id));

                    id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, "Products");
                    Assert.True(productsIds.TryAdd(id));

                    id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, typeof(User));
                    Assert.True(usersIds.TryAdd(id));

                    id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, new Product());
                    Assert.True(productsIds.TryAdd(id));

                    id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, new User());
                    Assert.True(usersIds.TryAdd(id));

                    id = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, typeof(Product));
                    Assert.True(productsIds.TryAdd(id));
                });

                Assert.Equal(count * 4, usersIds.Count);
                Assert.Equal(count * 4, productsIds.Count);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Capacity_Should_Double(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, "Raven/Hilo/users");

                    session.SaveChanges();

                    for (var i = 0; i < 32; i++)
                        await hiLoKeyGenerator.GenerateDocumentIdAsync(new User());
                }

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/Users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 96);

                    //we should be receiving a range of 64 now
                    await hiLoKeyGenerator.GenerateDocumentIdAsync(new User());
                }

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 160);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Return_Unused_Range_On_Dispose(Options options)
        {
            using (var store = GetDocumentStore())
            {
                var newStore = new DocumentStore()
                {
                    Urls = store.Urls,
                    Database = store.Database
                };
                newStore.Initialize();

                using (var session = newStore.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 32
                    }, "Raven/Hilo/users");

                    session.SaveChanges();

                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }
                newStore.Dispose(); //on document store dispose, hilo-return should be called 

                newStore = new DocumentStore()
                {
                    Urls = store.Urls,
                    Database = store.Database
                };
                newStore.Initialize();

                using (var session = newStore.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/users");
                    var max = hiloDoc.Max;
                    Assert.Equal(34, max);
                }
                newStore.Dispose();
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_Resolve_Conflict_With_Highest_Number(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var s1 = store1.OpenSession())
                {
                    var hiloDoc = new HiloDoc
                    {
                        Max = 128
                    };
                    s1.Store(hiloDoc, "Raven/Hilo/users");
                    s1.SaveChanges();

                    s1.Store(new User(), "marker/doc$Raven/Hilo/users");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    var hiloDoc2 = new HiloDoc
                    {
                        Max = 64
                    };
                    s2.Store(hiloDoc2, "Raven/Hilo/users");
                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                WaitForMarkerDocumentAndAllPrecedingDocumentsToReplicate(store2);

                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store2, store2.Database,
                    store2.Conventions.IdentityPartsSeparator);
                var nextId = await GetNextIdAsync(hiLoKeyGenerator);
                Assert.Equal(nextId, 129);
            }
        }

        private static void WaitForMarkerDocumentAndAllPrecedingDocumentsToReplicate(DocumentStore store2)
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                using (var session = store2.OpenSession())
                {
                    if (session.Load<object>("marker/doc$Raven/Hilo/users") != null)
                        break;
                    Thread.Sleep(32);
                }
                if (sp.Elapsed.TotalSeconds > (Debugger.IsAttached ? 60 * 1024 : 30))
                    throw new TimeoutException("waited too long");
            }
        }

        //hilo concurrency tests

        private const int GeneratedIdCount = 2000;
        private const int ThreadCount = 100;

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ParallelGeneration_NoClashesOrGaps(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var gen = new AsyncHiLoIdGenerator("When_generating_lots_of_keys_concurrently_there_are_no_clashes", store,
                    store.Database, store.Conventions.IdentityPartsSeparator);
                ConcurrencyTester(() => GetNextIdAsync(gen).GetAwaiter().GetResult(), ThreadCount, GeneratedIdCount);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SequentialGeneration_NoClashesOrGaps(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var gen = new AsyncHiLoIdGenerator("When_generating_lots_of_keys_concurrently_there_are_no_clashes", store,
                    store.Database, store.Conventions.IdentityPartsSeparator);
                ConcurrencyTester(() => GetNextIdAsync(gen).GetAwaiter().GetResult(), 1, GeneratedIdCount);
            }
        }

        private static void ConcurrencyTester(Func<long> generate, int threadCount, int generatedIdCount)
        {
            var waitingThreadCount = new CountdownEvent(threadCount);
            var starterGun = new ManualResetEvent(false);

            var results = new long[generatedIdCount];
            var threads = Enumerable.Range(0, threadCount).Select(threadNumber => new Thread(() =>
            {
                // Wait for all threads to be ready
                waitingThreadCount.Signal();
                starterGun.WaitOne();

                for (var i = threadNumber; i < generatedIdCount; i += threadCount)
                    results[i] = generate();
            })).ToArray();

            foreach (var t in threads)
                t.Start();

            // Wait for all tasks to reach the waiting stage
            waitingThreadCount.Wait(5000);

            // Start all the threads at the same time
            starterGun.Set();
            foreach (var t in threads)
                t.Join();

            var ids = new HashSet<long>();
            foreach (var value in results)
            {
                if (!ids.Add(value))
                {
                    throw new Exception("Id " + value + " was generated more than once, in indices "
                        + string.Join(", ", results.Select(Tuple.Create<long, int>).Where(x => x.Item1 == value).Select(x => x.Item2)));
                }
            }

            for (long i = 1; i <= GeneratedIdCount; i++)
                Assert.True(ids.Contains(i), "Id " + i + " was not generated.");
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLoKeyGenerator_works_without_aggressive_caching(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                Assert.Equal(1L, await GetNextIdAsync(hiLoKeyGenerator));
                Assert.Equal(2L, await GetNextIdAsync(hiLoKeyGenerator));
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLoKeyGenerator_async_hangs_when_aggressive_caching_enabled(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (store.AggressivelyCache())
                {
                    var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store, store.Database,
                        store.Conventions.IdentityPartsSeparator);

                    Assert.Equal(1L, await GetNextIdAsync(hiLoKeyGenerator));
                    Assert.Equal(2L, await GetNextIdAsync(hiLoKeyGenerator));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLoKeyGenerator_hangs_when_aggressive_caching_enabled(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (store.AggressivelyCache())
                {
                    var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store, store.Database,
                        store.Conventions.IdentityPartsSeparator);

                    Assert.Equal(1L, await GetNextIdAsync(hiLoKeyGenerator));
                    Assert.Equal(2L, await GetNextIdAsync(hiLoKeyGenerator));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLoKeyGenerator_hangs_when_aggressive_caching_enabled_on_other_documentstore(Options options)
        {
            using var server = GetNewServer();
            using var otherServer = GetNewServer();

            var o1 = options.Clone();
            var o2 = options.Clone();

            using (var store = GetDocumentStore(o1))
            using (var otherStore = GetDocumentStore(o2))
            {
                using (otherStore.AggressivelyCache()) // Note that we don't even use the other store, we just call AggressivelyCache on it
                {
                    var hilo = new AsyncHiLoIdGenerator("users", store, store.Database,
                        store.Conventions.IdentityPartsSeparator);
                    Assert.Equal(1L, await GetNextIdAsync(hilo));
                    Assert.Equal(2L, await GetNextIdAsync(hilo));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HiLoKeyGenerator_async_hangs_when_aggressive_caching_enabled_on_other_documentstore(Options options)
        {
            using var server = GetNewServer();
            using var otherServer = GetNewServer();

            var o1 = options.Clone();
            var o2 = options.Clone();

            using (var store = GetDocumentStore(o1))
            using (var otherStore = GetDocumentStore(o2))
            {
                using (otherStore.AggressivelyCache()) // Note that we don't even use the other store, we just call AggressivelyCache on it
                {
                    var hilo = new AsyncHiLoIdGenerator("users", store, store.Database,
                        store.Conventions.IdentityPartsSeparator);
                    Assert.Equal(1L, (await hilo.GetNextIdAsync()).Id);
                    Assert.Equal(2L, (await hilo.GetNextIdAsync()).Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReplicateHiLoTombstone(Options options)
        {
/////////////////////////
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "marker/doc");
                    s1.Store(new User { Name = "Egor" });
                    s1.SaveChanges();
                }
                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("Raven/Hilo/users");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    for (var i = 0; i < 32; i++)
                    {
                        s2.Store(new User
                        {
                            Name = $"user2_{i}"
                        });
                    }
                    s2.SaveChanges();
                }
                await SetupReplicationAsync(store1, store2);
                var marker = WaitForDocumentToReplicate<User>(store2, "marker/doc", 15 * 1000);
                Assert.NotNull(marker);
            }
        }

        private static async Task<long> GetNextIdAsync(AsyncHiLoIdGenerator idGenerator)
        {
            var nextId = await idGenerator.GetNextIdAsync();
            return nextId.Id;
        }
    }
}
