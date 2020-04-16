using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.ServerWide.Commands;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Replication
{
    public class ServerWideReplication : RavenTestBase
    {
        public ServerWideReplication(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [Fact]
        public async Task CanStoreServerWideExternalReplication()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                // the configuration is applied to existing databases
                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var externalReplications1 = record1.ExternalReplications;
                Assert.Equal(1, externalReplications1.Count);
                ValidateConfiguration(serverWideConfiguration, externalReplications1.First(), store.Database);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var externalReplications = record1.ExternalReplications;
                Assert.Equal(1, externalReplications.Count);
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateConfiguration(serverWideConfiguration, record2.ExternalReplications.First(), newDbName);

                // update the external replication configuration
                putConfiguration.TopologyDiscoveryUrls = new[] {store.Urls.First(), "http://localhost:8080"};
                putConfiguration.Name = serverWideConfiguration.Name;

                result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.ExternalReplications.Count);
                ValidateConfiguration(serverWideConfiguration, record1.ExternalReplications.First(), store.Database);

                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.ExternalReplications.Count);
                ValidateConfiguration(serverWideConfiguration, record2.ExternalReplications.First(), newDbName);
            }
        }

        private static void ValidateServerWideConfiguration(ServerWideExternalReplication serverWideConfiguration, ServerWideExternalReplication putConfiguration)
        {
            Assert.Equal(serverWideConfiguration.Name, putConfiguration.Name ?? putConfiguration.GetDefaultTaskName());
            Assert.Equal(putConfiguration.Disabled, serverWideConfiguration.Disabled);
            Assert.True(putConfiguration.TopologyDiscoveryUrls.SequenceEqual(serverWideConfiguration.TopologyDiscoveryUrls));
        }

        private static void ValidateConfiguration(ServerWideExternalReplication serverWideConfiguration, ExternalReplication externalReplication, string databaseName)
        {
            Assert.Equal(PutServerWideExternalReplicationCommand.GetTaskName(serverWideConfiguration.Name), externalReplication.Name);
            Assert.Equal(serverWideConfiguration.Disabled, externalReplication.Disabled);
            Assert.Equal(databaseName, externalReplication.Database);
            Assert.Equal(PutServerWideExternalReplicationCommand.GetRavenConnectionStringName(serverWideConfiguration.Name), externalReplication.ConnectionStringName);
        }
    }
}
