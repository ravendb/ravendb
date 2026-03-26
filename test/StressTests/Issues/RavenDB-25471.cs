using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Issues
{
    public class RavenDB_25471 : RavenTestBase
    {
        public RavenDB_25471(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { BackupType.Backup })]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { BackupType.Snapshot })]
        public async Task DisableOlapOnRestoreWithoutLicense(Options options, BackupType backupType)
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.DisableRevisionCompression(Server, store);

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<OlapConnectionString>(new OlapConnectionString
                {
                    Name = "olap-cs"
                }));

                var olapEtlConfiguration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = "olap-cs",
                    Transforms = { new Transformation { Name = "loadAll", Collections = { "Users" }, Script = "loadToUsers(this)" } }
                };
                var operationResult = await store.Maintenance.SendAsync(new AddEtlOperation<OlapConnectionString>(olapEtlConfiguration));

                var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                olapEtlConfiguration.Disabled = true;
                await store.Maintenance.SendAsync(new UpdateEtlOperation<OlapConnectionString>(operationResult.TaskId, olapEtlConfiguration));

                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_COMM);
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store,
                       new RestoreBackupConfiguration
                       {
                           BackupLocation = Directory.GetDirectories(backupPath).First(),
                           DatabaseName = databaseName,
                           DisableOngoingTasks = true
                       }))
                {
                    var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(1, record.OlapEtls.Count);
                    Assert.True(record.OlapEtls[0].Disabled);
                }
            }
        }
    }
}
