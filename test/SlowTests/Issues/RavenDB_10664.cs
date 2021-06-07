using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10664 : RavenTestBase
    {
        public RavenDB_10664(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AutoNamingAlgorithmOfOngoingTasksShouldTakeNameAlreadyExistsIntoAccount()
        {
            using (var store = GetDocumentStore())
            {
                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";

                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] {"http://127.0.0.1:12345"}
                };

                var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dbName, csName)));
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dbName, csName)));
                
                var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", azureSettings: new AzureSettings
                {
                    StorageContainer = "abc"
                }, disabled: true);

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


                // for Pull Replication Hub name is required - no need to test

                var sink = new PullReplicationAsSink
                {
                    HubName = "aa",
                    ConnectionString = connectionString,
                    ConnectionStringName = connectionString.Name
                };

                await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink));
                await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink));
            }
        } 
    }
}
