using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Identity;
using Raven.Server.Documents.Handlers;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Client
{
    public class ShardedHiloTests : ReplicationTestBase
    {
        public ShardedHiloTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanStoreWithoutId()
        {
            using (var store = Sharding.GetDocumentStore())
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

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task ReshardingOfHiloDoc()
        {
            var sw = Stopwatch.StartNew();
            using (var store = Sharding.GetDocumentStore())
            {
                var hiloDocId = HiLoHandler.RavenHiloIdPrefix + "Users";
                int start = 1, end = 20;

                for (int i = start; i <= end; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User();
                        session.Store(user);
                        session.SaveChanges();

                        var id = user.Id;
                        var indexOf = id.IndexOf('/') + 1;
                        var number = int.Parse(id.Substring(indexOf, i < 10 ? 1 : 2));
                        Assert.Equal(i, number);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDocument>(hiloDocId);
                    Assert.Equal(32, hiloDoc.Max);
                }

                var originalLocation = await Sharding.GetShardNumberForAsync(store, hiloDocId);

                await Sharding.Resharding.MoveShardForId(store, hiloDocId);
                
                var newLocation = await Sharding.GetShardNumberForAsync(store, hiloDocId);
                Assert.NotEqual(originalLocation, newLocation);

                var originalShard = ShardHelper.ToShardName(store.Database, originalLocation);
                var newShard = ShardHelper.ToShardName(store.Database, newLocation);

                using (var session = store.OpenSession(originalShard))
                {
                    var hiloDoc = session.Load<HiloDocument>(hiloDocId);
                    Assert.Null(hiloDoc);
                }
                using (var session = store.OpenSession(newShard))
                {
                    var hiloDoc = session.Load<HiloDocument>(hiloDocId);
                    Assert.Equal(32, hiloDoc.Max);
                }

                start = end + 1;
                end *= 2;

                for (int i = start; i <= end; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User();
                        session.Store(user);
                        session.SaveChanges();

                        var id = user.Id;
                        var indexOf = id.IndexOf('/') + 1;
                        var number = int.Parse(id.Substring(indexOf, 2));
                        Assert.Equal(i, number);
                    }
                }

                using (var session = store.OpenSession(newShard))
                {
                    var hiloDoc = session.Load<HiloDocument>(hiloDocId);
                    Assert.Equal(96, hiloDoc.Max);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task AfterReshardingOfHiloDoc_ShouldResolveHiloConflictWithHighestNumber()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var hiloDocId = HiLoHandler.RavenHiloIdPrefix + "Users";

                using (var s1 = store1.OpenSession())
                {
                    var hiloDoc = new HiloDocument
                    {
                        Max = 128
                    };
                    s1.Store(hiloDoc, hiloDocId);
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    var hiloDoc2 = new HiloDocument
                    {
                        Max = 64
                    };
                    s2.Store(hiloDoc2, hiloDocId);
                    s2.SaveChanges();
                }

                await Sharding.Resharding.MoveShardForId(store2, hiloDocId);

                await SetupReplicationAsync(store1, store2);
                await Sharding.Replication.EnsureReplicatingAsyncForShardedDestination(store1, store2);

                var hiLoKeyGenerator = new AsyncHiLoIdGenerator("users", store2, store2.Database,
                    store2.Conventions.IdentityPartsSeparator);
                var nextId = (await hiLoKeyGenerator.GetNextIdAsync()).Id;
                Assert.Equal(129, nextId);
            }
        }
    }
}
