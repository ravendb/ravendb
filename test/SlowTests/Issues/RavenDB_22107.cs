using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Sharding.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22107 : ReplicationTestBase
    {
        public RavenDB_22107(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task GetReplicationActiveConnectionsShouldNotThrow(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "Shiran" }, "users/1");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                var db = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "users/1");
                var replicationActiveConnections = await store1.Maintenance.ForDatabase(db.Name).SendAsync(new ShardedExternalReplicationTests.GetReplicationActiveConnectionsInfoOperation());

                var expectedConnectionsCount = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3;
                Assert.NotNull(replicationActiveConnections.IncomingConnections);
                Assert.Equal(expectedConnectionsCount, replicationActiveConnections.IncomingConnections.Count);

                expectedConnectionsCount = 1;
                Assert.NotNull(replicationActiveConnections.OutgoingConnections);
                Assert.Equal(expectedConnectionsCount, replicationActiveConnections.OutgoingConnections.Count);
            }
        }
    }
}
