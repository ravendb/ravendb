using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Sharding.Replication;
using Sparrow.Json;
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

                var outgoingConnection = replicationActiveConnections.OutgoingConnections.Single();

                Assert.True(outgoingConnection is ExternalReplication);
                Assert.Equal(TimeSpan.Zero, ((ExternalReplication)outgoingConnection)!.DelayReplicationFor);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task GetReplicationOutgoingsFailureInfoShouldNotThrow(Options options)
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
                await BreakReplication(Server.ServerStore, db.Name);

                var replicationFailureInfo = await store1.Maintenance.ForDatabase(db.Name).SendAsync(new GetReplicationOutgoingsFailureInfoOperation());

                Assert.NotNull(replicationFailureInfo.Stats);
                Assert.Equal(1, replicationFailureInfo.Stats.Count);

                var info = replicationFailureInfo.Stats.Single();

                Assert.True(info.Key is ExternalReplication);
                Assert.Equal(TimeSpan.Zero, ((ExternalReplication)info.Key)!.DelayReplicationFor);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task GetReplicationOutgoingReconnectionQueueShouldNotThrow(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "Shiran" }, "users/1");
                    s1.SaveChanges();
                }

                var delay = TimeSpan.FromSeconds(5);
                var externalTask = new ExternalReplication(store2.Database, "DelayedExternalReplication")
                {
                    DelayReplicationFor = delay
                };
                await AddWatcherToReplicationTopology(store1, externalTask);
                await EnsureReplicatingAsync(store1, store2);

                var source = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "users/1");
                var destination = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "users/1");

                destination.Dispose();

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "Shiran 2" }, "users/2$users/1");
                    s1.SaveChanges();
                }

                ReplicationOutgoingReconnectionQueuePreview info = null;
                await WaitAndAssertForValueAsync(async () =>
                {
                    info = await store1.Maintenance.ForDatabase(source.Name).SendAsync(new GetReplicationOutgoingReconnectionQueueOperation());
                    return info.QueueInfo.Count;
                }, 1);

                var replicationNode = info.QueueInfo.Single();
                Assert.True(replicationNode is ExternalReplication);
                Assert.Equal(delay, ((ExternalReplication)replicationNode)!.DelayReplicationFor);
            }
        }

        internal class GetReplicationOutgoingsFailureInfoOperation : IMaintenanceOperation<ReplicationOutgoingsFailurePreview>
        {
            public RavenCommand<ReplicationOutgoingsFailurePreview> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetReplicationOutgoingsFailureInfoCommand();
            }
        }

        internal class GetReplicationOutgoingReconnectionQueueOperation : IMaintenanceOperation<ReplicationOutgoingReconnectionQueuePreview>
        {
            public RavenCommand<ReplicationOutgoingReconnectionQueuePreview> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetReplicationOutgoingReconnectionQueueCommand();
            }
        }
    }
}
