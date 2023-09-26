using System;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_21315 : ReplicationTestBase
    {
        public RavenDB_21315(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ChangeVectorWithMoveTagShouldNotLeakToNonShardedAfterResharding()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var id = "users/shiran";

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Shiran"}, id);
                    await session.SaveChangesAsync();
                }

                await Sharding.Resharding.MoveShardForId(store1, id);

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var stats2 = await GetDatabaseStatisticsAsync(store2);
                Assert.NotNull(stats2.DatabaseChangeVector);
                Assert.False(stats2.DatabaseChangeVector.Contains("MOVE", StringComparison.OrdinalIgnoreCase));
            }
        }
    }
}
