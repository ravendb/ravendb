using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15772 : ClusterTestBase
    {
        public RavenDB_15772(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldntThrowConcurrencyException()
        {
            using var store = GetDocumentStore();
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var fixedOrder = record.Topology.AllNodes.ToList();

            var updateClientConfig = store.Maintenance.SendAsync(
                new PutClientConfigurationOperation(
                    new ClientConfiguration
                    {
                        MaxNumberOfRequestsPerSession = 345
                    }));

            var updateTopology = store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, true));
            await Task.WhenAll(updateClientConfig, updateTopology);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record1 = Server.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                Assert.Equal(345, record1.Client.MaxNumberOfRequestsPerSession);

                var fixedOrder1 = record1.Topology.AllNodes.ToList();
                Assert.Equal(1, fixedOrder1.Count);
                Assert.True(fixedOrder1.Contains(fixedOrder[0]));
                Assert.True(fixedOrder.Contains(fixedOrder1[0]));
            }
        }

        [Fact]
        public async Task PutDatabaseClientConfigurationCommandTest()
        {
            var (_, leader) = await CreateRaftCluster(2);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });


            // var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var databaseName = store.Database;
            // var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            var command = new PutDatabaseClientConfigurationCommand(new ClientConfiguration
            {
                MaxNumberOfRequestsPerSession = 345
            }, databaseName, RaftIdGenerator.NewId());
            long index = (await leader.ServerStore.SendToLeaderAsync(command)).Index;
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));

            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record1 = leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                Assert.Equal(345, record1.Client.MaxNumberOfRequestsPerSession);
            }
        }

        [Fact]
        public async Task PutDatabaseSettingsCommandTest()
        {
            var (_, leader) = await CreateRaftCluster(2);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
            
        
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal("true", record.Settings["ThrowIfAnyIndexCannotBeOpened"]);

            var databaseName = store.Database;
            // var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
        
            var command = new PutDatabaseSettingsCommand(new Dictionary<string, string>() { { "ThrowIfAnyIndexCannotBeOpened", "false" } }, databaseName, RaftIdGenerator.NewId());
            long index = (await leader.ServerStore.SendToLeaderAsync(command)).Index;
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));
        
            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record1 = leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                Assert.Equal("false", record1.Settings["ThrowIfAnyIndexCannotBeOpened"]);
            }
        }

        [Fact]
        public async Task PutDatabaseStudioConfigurationCommandTest()
        {
            var (_, leader) = await CreateRaftCluster(2);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });


            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal("true", record.Settings["ThrowIfAnyIndexCannotBeOpened"]);

            var databaseName = store.Database;
            // var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            var command = new PutDatabaseStudioConfigurationCommand(new ServerWideStudioConfiguration()
            {
                DisableAutoIndexCreation = true,
            }, databaseName, RaftIdGenerator.NewId());
            long index = (await leader.ServerStore.SendToLeaderAsync(command)).Index;
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));

            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record1 = leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                Assert.Equal(true, record1.Studio.DisableAutoIndexCreation);
            }
        }

    }
}
