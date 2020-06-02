using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
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
