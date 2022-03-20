using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15437 : ReplicationTestBase
    {
        public RavenDB_15437(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldNotHaveCounterAndCountersSnapshotInMetadata()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Rhino"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 100);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                var dbA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                dbA.Configuration.Replication.MaxItemsCount = 1;
                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();

                using (var session = storeA.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    // add revision to A
                    session.CountersFor(user).Increment("Likes2", 200);
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    await session.SaveChangesAsync();
                }

                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    session.CountersFor(user).Increment("Likes3", 300);
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<BlittableJsonReaderObject>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Counters));
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.RevisionCounters));
                }

                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                using (var sessionA = storeA.OpenAsyncSession())
                using (var sessionB = storeB.OpenAsyncSession())
                {
                    var countersA = await sessionA.CountersFor("users/1").GetAllAsync();
                    var countersB = await sessionB.CountersFor("users/1").GetAllAsync();

                    Assert.Equal(4, countersA.Count);
                    Assert.Equal(4, countersB.Count);

                    foreach (var counterA in countersA)
                    {
                        Assert.True(countersB.ContainsKey(counterA.Key));

                        countersB.TryGetValue(counterA.Key, out var val);
                        Assert.Equal(val, counterA.Value);
                    }
                }
            }
        }
    }
}
