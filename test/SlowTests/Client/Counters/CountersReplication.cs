using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersReplication : ReplicationTestBase
    {
        [Fact]
        public async Task ConflictsInMetadata()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                await storeB.Counters.IncrementAsync("users/1-A", "likes", 14);
                await storeB.Counters.IncrementAsync("users/1-A", "dislikes", 13);

                await storeA.Counters.IncrementAsync("users/1-A", "likes", 12);
                await storeA.Counters.IncrementAsync("users/1-A", "cats", 11);

                EnsureReplicating(storeA, storeB);

                var val = await storeB.Counters.GetAsync("users/1-A", "likes");
                Assert.Equal(26, val);

                val = await storeB.Counters.GetAsync("users/1-A", "dislikes");
                Assert.Equal(13, val);

                val = await storeB.Counters.GetAsync("users/1-A", "cats");
                Assert.Equal(11, val);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    var counters = (object[])session.Advanced.GetMetadataFor(user)["@counters"];

                    Assert.Equal(3, counters.Length);
                    // verify that counters are sorted
                    Assert.Equal("cats", counters[0]);
                    Assert.Equal("dislikes", counters[1]);
                    Assert.Equal("likes", counters[2]);

                }
            }
        }

        [Fact]
        public async Task CounterTombstonesReplication1()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/2-A");

                    session.CountersFor("users/1-A").Increment("likes", 10);
                    session.CountersFor("users/1-A").Increment("dislikes", 20);
                    session.CountersFor("users/2-A").Increment("downloads", 30);

                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/2-A").Delete("downloads");

                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                var val = await storeB.Counters.GetAsync("users/1-A", "likes");
                Assert.Null(val);

                val = await storeB.Counters.GetAsync("users/1-A", "dislikes");
                Assert.Equal(20, val);

                val = await storeB.Counters.GetAsync("users/2-A", "downloads");
                Assert.Null(val);

                var db = await GetDocumentDatabaseInstanceFor(storeB);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(2, tombstones.Count);
                    Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[0].Type);
                    Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[1].Type);
                }
            }
        }

        [Fact]
        public async Task CounterTombstonesReplication2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/2-A");

                    session.CountersFor("users/1-A").Increment("likes", 10);
                    session.CountersFor("users/1-A").Increment("dislikes", 20);
                    session.CountersFor("users/2-A").Increment("downloads", 30);

                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/2-A").Delete("downloads");

                    await session.SaveChangesAsync();
                }

                // we intentionally setup the replication after counters were 
                // deleted from storage and counter tombstones were created 

                await SetupReplicationAsync(storeA, storeB);         
                EnsureReplicating(storeA, storeB);

                var val = await storeB.Counters.GetAsync("users/1-A", "likes");
                Assert.Null(val);

                val = await storeB.Counters.GetAsync("users/1-A", "dislikes");
                Assert.Equal(20, val);

                val = await storeB.Counters.GetAsync("users/2-A", "downloads");
                Assert.Null(val);

                var db = await GetDocumentDatabaseInstanceFor(storeB);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(2, tombstones.Count);
                    Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[0].Type);
                    Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[1].Type);
                }
            }
        }
    }
}
