using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25235 :RavenTestBase
    {
        public RavenDB_25235(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task RestoreDisabledRavenEtl()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";

            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    RavenEtlConfiguration etlConfiguration = new()
                    {
                        Name = csName,
                        ConnectionStringName = csName,
                        Transforms = { new Transformation { Name = $"ETL : {csName}", ApplyToAllDocuments = true } },
                        MentorNode = "A",
                        Disabled = true
                    };
                    var connectionString = new RavenConnectionString
                    {
                        Name = csName,
                        Database = dbName,
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" },
                    };

                    Assert.NotNull(store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString)));
                    store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    runRestore(store, backupPath);
                    var recored = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database + "1"));
                    Assert.Equal(1, recored.RavenEtls.Count);
                    Assert.True(recored.RavenEtls[0].Disabled);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        private void runRestore(DocumentStore store, string backupPath)
        {
            var configuration = new RestoreBackupConfiguration { DatabaseName = store.Database + "1" };
            configuration.BackupLocation = Directory.GetDirectories(backupPath).First();
            Backup.RestoreDatabase(store, configuration);
        }

        private static async Task RunBackup(DocumentStore store, string backupPath)
        {
            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            _ = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        }
    }
}
