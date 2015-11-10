using Raven.Abstractions.Data;
using Raven.Backup;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Tests.Common;
using Raven.Tests.Helpers;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1520 : RavenTest
    {
        private readonly string BackupDir;
        private readonly string DataDir;

        public RavenDB_1520()
        {
            BackupDir = NewDataPath("BackupDatabase");
            DataDir = NewDataPath("DataDir");
        }

        [Theory]
        [PropertyData("Storages")]
        public void Backup_and_restore_of_system_database_should_work(string storage)
        {
            using (var ravenServer = GetNewServer(runInMemory: false,requestedStorage: storage))
            using (var _ = NewRemoteDocumentStore(ravenDbServer: ravenServer, databaseName: "fooDB", runInMemory: false))
            {
                using (var systemDatabaseBackupOperation = new DatabaseBackupOperation(new BackupParameters
                                                                                       {
                                                                                           BackupPath = BackupDir,
                                                                                           Database = Constants.SystemDatabase,
                                                                                           ServerUrl = ravenServer.SystemDatabase.Configuration.ServerUrl
                                                                                       }))
                {

                    Assert.True(systemDatabaseBackupOperation.InitBackup());
                    WaitForBackup(ravenServer.SystemDatabase, true);
                }
            }

            Assert.DoesNotThrow(() => MaintenanceActions.Restore(new AppSettingsBasedConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir
            }, s => { }));

        }
    }
}
