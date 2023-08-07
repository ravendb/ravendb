using System.Threading.Tasks;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15224 : ReplicationTestBase
    {
        public RavenDB_15224(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldReplicateCounterDelete(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                // create document with counter one A
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(storeA, storeB);

                // delete counter on B
                using (var session = storeB.OpenAsyncSession())
                {
                    var countersFor = session.CountersFor("users/1");
                    var val = await countersFor.GetAsync("likes");
                    Assert.Equal(10, val);

                    countersFor.Delete("likes");
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(storeB, storeA);

                // counter should be deleted on A
                using (var session = storeA.OpenAsyncSession())
                {
                    var countersFor = session.CountersFor("users/1");
                    var val = await countersFor.GetAsync("likes");
                    Assert.Null(val);
                }
            }
        }
    }
}
