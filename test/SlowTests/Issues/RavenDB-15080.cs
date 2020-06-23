using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15080 : ReplicationTestBase
    {
        public RavenDB_15080(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanSplitLowerCasedAndUpperCasedCounterNames()
        {
            using (var storeA = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv1"
                    }, "users/1");

                    var countersFor = session.CountersFor("users/1");

                    for (int i = 0; i < 500; i++)
                    {
                        var str = $"abc{i}";
                        countersFor.Increment(str);
                    }

                    await session.SaveChangesAsync();
                }


                using (var session = storeA.OpenAsyncSession())
                {
                    var countersFor = session.CountersFor("users/1");

                    for (int i = 0; i < 500; i++)
                    {
                        var str = $"Xyz{i}";
                        countersFor.Increment(str);
                    }

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanSplitAndReplicateRandomCounterName()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }
                EnsureReplicating(storeA, storeB);
                var t1 = Task.Run(async () =>
                {
                    var rand = new Random(42);
                    for (int i = 0; i < 1_000; i++)
                    {
                        using (var session = storeA.OpenAsyncSession())
                        {
                            var str = RandomString(8, rand);
                            session.CountersFor("users/1").Increment(str, 100000);
                            await session.SaveChangesAsync();
                        }
                    }
                });
                var t2 = Task.Run(async () =>
                {
                    var rand = new Random(357);
                    for (int i = 0; i < 1_000; i++)
                    {
                        using (var session = storeB.OpenAsyncSession())
                        {
                            var str = RandomString(8, rand);
                            session.CountersFor("users/1").Increment(str, 100000);
                            await session.SaveChangesAsync();
                        }
                    }
                });
                await Task.WhenAll(t1, t2);

                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);
                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);
            }
        }

        [Fact]
        public void CounterOperationsShouldBeCaseInsensitiveToCounterName()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("abc");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should NOT create a new counter
                    session.CountersFor("users/1").Increment("ABc");
                    session.SaveChanges();
                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var all = db.DocumentsStorage.CountersStorage.GetCountersForDocument(ctx, "users/1")?.ToList();
                    Assert.Equal(1, all?.Count);
                    Assert.Equal("abc", all?[0]);
                }

                using (var session = store.OpenSession())
                {
                    // get should be case-insensitive to counter name

                    var val = session.CountersFor("users/1").Get("AbC");
                    Assert.True(val.HasValue);
                    Assert.Equal(2, val);

                    var doc = session.Load<User>("users/1");
                    var counterNames = session.Advanced.GetCountersFor(doc);
                    Assert.Equal(1, counterNames.Count);
                    Assert.Equal("abc", counterNames[0]); // metadata counter-names should preserve their original casing 
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("XyZ");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1").Get("xyz");
                    Assert.True(val.HasValue);
                    Assert.Equal(1, val);

                    var doc = session.Load<User>("users/1");
                    var counterNames = session.Advanced.GetCountersFor(doc);
                    Assert.Equal(2, counterNames.Count);

                    // metadata counter-names should preserve their original casing
                    Assert.Equal("abc", counterNames[0]);
                    Assert.Equal("XyZ", counterNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    // delete should be case-insensitive to counter name

                    session.CountersFor("users/1").Delete("aBC");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1").Get("abc");
                    Assert.Null(val);

                    var doc = session.Load<User>("users/1");
                    var counterNames = session.Advanced.GetCountersFor(doc);
                    Assert.Equal(1, counterNames.Count);
                    Assert.Equal("XyZ", counterNames[0]);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("xyZ");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1").Get("Xyz");
                    Assert.Null(val);

                    var doc = session.Load<User>("users/1");
                    var counterNames = session.Advanced.GetCountersFor(doc);
                    Assert.Null(counterNames);
                }
            }
        }

        [Fact]
        public async Task CountersShouldBeCaseInsensitive()
        {
            // RavenDB-14753

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("Likes", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("lIkEs");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    var counters = session.CountersFor(company).GetAll();

                    Assert.Equal(0, counters.Count);
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                var countersStorage = database.DocumentsStorage.CountersStorage;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var values = countersStorage
                        .GetCounterPartialValues(context, "companies/1", "Likes")
                        .ToList();

                    Assert.Equal(0, values.Count);
                }
            }
        }

        [Fact]
        public void DeletedCounterShouldNotBePresentInMetadataCounters()
        {
            // RavenDB-14753

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("Likes", 999);
                    session.CountersFor(company).Increment("Cats", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("lIkEs");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    company.Name = "RavenDB";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    var counters = session.Advanced.GetCountersFor(company);

                    Assert.Equal(1, counters.Count);
                    Assert.Equal("Cats", counters[0]);
                }

            }
        }

        [Fact]
        public void GetCountersForDocumentShouldReturnNamesInTheirOriginalCasing()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    var countersFor = session.CountersFor("users/1");
                    countersFor.Increment("AviV");
                    countersFor.Increment("Karmel");
                    countersFor.Increment("PAWEL");

                    session.SaveChanges();
                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var all = db.DocumentsStorage.CountersStorage.GetCountersForDocument(ctx, "users/1")?.ToList();
                    Assert.Equal(3, all?.Count);
                    Assert.Equal("AviV", all[0]);
                    Assert.Equal("Karmel", all[1]);
                    Assert.Equal("PAWEL", all[2]);
                }

                using (var session = store.OpenSession())
                {
                    // GetAll should return counter names in their original casing
                    var all = session.CountersFor("users/1").GetAll();
                    Assert.Equal(3, all?.Count);

                    var keys = all.Keys.ToList();
                    Assert.True(keys.Contains("AviV"));
                    Assert.True(keys.Contains("Karmel"));
                    Assert.True(keys.Contains("PAWEL"));
                }
            }
        }

        [Fact]
        public void CanDeleteAndReInsertCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("Likes", 999);
                    session.CountersFor(company).Increment("Cats", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("Likes");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    var counters = session.Advanced.GetCountersFor(company);

                    Assert.Equal(1, counters.Count);
                    Assert.Equal("Cats", counters[0]);

                    var counter = session.CountersFor(company).Get("Likes");

                    Assert.Null(counter);

                    var all = session.CountersFor(company).GetAll();
                    Assert.Equal(1, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("companies/1").Increment("Likes");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    var counters = session.Advanced.GetCountersFor(company);

                    Assert.Equal(2, counters.Count);
                    Assert.Equal("Cats", counters[0]);
                    Assert.Equal("Likes", counters[1]);

                    var counter = session.CountersFor(company).Get("Likes");

                    Assert.NotNull(counter);
                    Assert.Equal(1, counter.Value);
                }

            }
        }

        [Fact]
        public void CountersSessionCacheShouldBeCaseInsensitiveToCounterName()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("Likes", 333);
                    session.CountersFor(company).Increment("Cats", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    // the document is now tracked by the session,
                    // so now counters-cache has access to '@counters' from metadata 

                    // searching for the counter's name in '@counters' should be done in a case insensitive manner
                    // counter name should be found in '@counters' => go to server
                    var counter = session.CountersFor(company).Get("liKes");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.NotNull(counter);
                    Assert.Equal(333, counter.Value);

                    counter = session.CountersFor(company).Get("cats");
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.NotNull(counter);
                    Assert.Equal(999, counter.Value);
                }
            }
        }

        [Fact]
        public async Task ExportAndImportCountersShouldKeepOriginalCasing()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes", 100);
                        session.CountersFor("users/1").Increment("Dislikes", 200);
                        session.CountersFor("users/2").Increment("Downloads", 500);

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        var dic = await session.CountersFor(user1).GetAllAsync();
                        Assert.Equal(2, dic.Count);
                        Assert.Equal(100, dic["likes"]);
                        Assert.Equal(200, dic["dislikes"]);

                        var val = await session.CountersFor(user2).GetAsync("downloads");
                        Assert.Equal(500, val);
                    }

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var counterNames = session.Advanced.GetCountersFor(user1);
                        Assert.NotNull(counterNames);
                        Assert.Equal(2, counterNames.Count);
                        Assert.Equal("Dislikes", counterNames[0]);
                        Assert.Equal("Likes", counterNames[1]);

                        var user2 = await session.LoadAsync<User>("users/2");
                        counterNames = session.Advanced.GetCountersFor(user2);
                        Assert.NotNull(counterNames);
                        Assert.Equal(1, counterNames.Count);
                        Assert.Equal("Downloads", counterNames[0]);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private static string RandomString(int size, Random random)
        {
            var builder = new StringBuilder();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(74 * random.NextDouble() + 48)));
                builder.Append(ch);
            }
            return builder.ToString();
        }
    }
}
