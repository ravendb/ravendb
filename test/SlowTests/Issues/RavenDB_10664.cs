using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10664 : RavenTestBase
    {
        [Fact]
        public async Task AutoNamingAlgorithmOfOngoingTasksShouldTakeNameAlreadyExistsIntoAccount()
        {
            using (var store = GetDocumentStore())
            {
                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                }));
                
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dbName, csName)));
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dbName, csName)));
                
                var backupConfig = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath(suffix: "BackupFolder")
                    },
                    AzureSettings = new AzureSettings
                    {
                        StorageContainer = "abc"
                    },
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    Disabled = true
                };

                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));

                var etlConfiguration = new RavenEtlConfiguration
                {
                    ConnectionStringName = csName,
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "loadAll",
                            Collections = {"Users"},
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
            }
        } 
    }
}
