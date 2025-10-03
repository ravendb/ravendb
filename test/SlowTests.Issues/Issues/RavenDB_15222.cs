using System.Threading.Tasks;
using Raven.Client;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15222 : ReplicationTestBase
    {
        public RavenDB_15222(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldNotHaveCounterSnapshotInMetadata(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                // create a conflict

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                // conflict will be resolved to latest (incoming doc from A to B)
                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<BlittableJsonReaderObject>("users/1");
                    Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject md));
                    Assert.False(md.TryGet(Constants.Documents.Metadata.RevisionCounters, out object countersSnapshot));
                }
            }
        }
    }
}
