using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Identity;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class ShardedHiloTests : ShardedTestBase
    {
        public ShardedHiloTests(ITestOutputHelper output) : base(output)
        {
        }

        private class HiloDoc
        {
            public long Max { get; set; }
        }

        [Fact]
        public void CanStoreWithoutId()
        {
            using (var store = GetShardedDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Aviv" };
                    session.Store(user);

                    id = user.Id;
                    Assert.NotNull(id);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(id);
                    Assert.Equal("Aviv", loaded.Name);
                }
            }
        }

        [Fact]
        public async Task Hilo_Cannot_Go_Down()
        {
            const string hiloDocId = "Raven/Hilo/users";

            using (var store = GetShardedDocumentStore())
            {
                var hiloDoc = new HiloDoc
                {
                    Max = 32
                };

                using (var session = store.OpenSession())
                {
                    session.Store(hiloDoc, hiloDocId);
                    session.SaveChanges();
                }

                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", (DocumentStore)store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                var ids = new HashSet<long>
                {
                    await hiLoKeyGenerator.NextIdAsync()
                };

                using (var session = store.OpenSession())
                {
                    hiloDoc.Max = 12;
                    session.Store(hiloDoc, hiloDocId);
                    session.SaveChanges();
                }

                for (int i = 0; i < 128; i++)
                {
                    var nextId = await hiLoKeyGenerator.NextIdAsync();
                    Assert.True(ids.Add(nextId), "Failed at " + i);
                }

                var list = ids
                    .GroupBy(x => x)
                    .Select(g => new
                    {
                        g.Key,
                        Count = g.Count()
                    })
                    .Where(x => x.Count > 1)
                    .ToList();

                Assert.Empty(list);
            }
        }

        [Fact]
        public async Task HiLo_Async_MultiDb()
        {
            using (var store = GetShardedDocumentStore())
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
                }

                var multiDbHiLo = new AsyncMultiDatabaseHiLoIdGenerator((DocumentStore)store);

                var generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(store.Database, new User());
                Assert.Equal("users/65-A", generateDocumentKey);

                generateDocumentKey = await multiDbHiLo.GenerateDocumentIdAsync(store.Database, new Product());
                Assert.Equal("products/129-A", generateDocumentKey);
            }
        }

        [Fact]
        public async Task Capacity_Should_Double()
        {
            const string hiloDocId = "Raven/Hilo/users";

            using (var store = GetShardedDocumentStore())
            {
                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", (DocumentStore)store, store.Database,
                    store.Conventions.IdentityPartsSeparator);

                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, hiloDocId);

                    session.SaveChanges();
                }

                for (var i = 0; i < 32; i++)
                    await hiLoKeyGenerator.GenerateDocumentIdAsync(null);

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>(hiloDocId);
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 96);
                }

                //we should be receiving a range of 64 now
                await hiLoKeyGenerator.GenerateDocumentIdAsync(null);

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>(hiloDocId);
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 160);
                }
            }
        }

        [Fact]
        public void Return_Unused_Range_On_Dispose()
        {
            const string hiloDocId = "Raven/Hilo/users";

            using (var store = GetShardedDocumentStore())
            {
                var newStore = new DocumentStore
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
                    }, hiloDocId);

                    session.SaveChanges();

                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }
                newStore.Dispose(); //on document store dispose, hilo-return should be called 

                newStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                };
                newStore.Initialize();

                using (var session = newStore.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>(hiloDocId);
                    var max = hiloDoc.Max;
                    Assert.Equal(34, max);
                }
                newStore.Dispose();
            }
        }
    }
}
