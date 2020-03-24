using FastTests;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class ServerWideReplication : RavenTestBase
    {
        public ServerWideReplication(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        /*[Fact]
        public async Task CanStoreServerWideExternalReplication()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    Database = "test",
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
                ValidateBackupConfiguration(serverWideConfiguration, externalReplications1.First(), store.Database);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var backups2 = record1.PeriodicBackups;
                Assert.Equal(1, backups2.Count);
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);

                // update the external replication configuration
                putConfiguration.Database = "test2";
                putConfiguration.Name = serverWideConfiguration.Name;

                result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.PeriodicBackups.Count);
                ValidateBackupConfiguration(serverWideConfiguration, record1.PeriodicBackups.First(), store.Database);

                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.PeriodicBackups.Count);
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);
            }
        }
    }*/
    }
}
