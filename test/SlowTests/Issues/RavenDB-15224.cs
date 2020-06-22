using System.Threading.Tasks;
using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15224 : ReplicationTestBase
    {
        public RavenDB_15224(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldReplicateCounterDelete()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
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

                EnsureReplicating(storeA, storeB);

                // delete counter on B
                using (var session = storeB.OpenAsyncSession())
                {
                    var countersFor = session.CountersFor("users/1");
                    var val = await countersFor.GetAsync("likes");
                    Assert.Equal(10, val);

                    countersFor.Delete("likes");
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeB, storeA);

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
