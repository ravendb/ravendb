using Raven.Abstractions.Data;
using Raven.Backup;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1520 : RavenTestBase
	{
		private readonly string BackupDir;
		private readonly string DataDir;

		public RavenDB_1520()
		{
			BackupDir = NewDataPath("BackupDatabase");
			DataDir = NewDataPath("DataDir");
		}

		[Fact]
		public void Backup_and_restore_of_system_database_should_work()
		{
			using (var ravenServer = GetNewServer(runInMemory: false,requestedStorage:"esent"))
			using (var _ = NewRemoteDocumentStore(ravenDbServer: ravenServer, databaseName: "fooDB", runInMemory: false))
			{
			    using (var systemDatabaseBackupOperation = new BackupOperation
			                                            {
			                                                BackupPath = BackupDir,
			                                                Database = Constants.SystemDatabase,
			                                                ServerUrl = ravenServer.SystemDatabase.Configuration.ServerUrl
			                                            })
			    {

			        Assert.True(systemDatabaseBackupOperation.InitBackup());
			        WaitForBackup(ravenServer.SystemDatabase, true);
			    }
			}

			Assert.DoesNotThrow(() => MaintenanceActions.Restore(new RavenConfiguration(), new RestoreRequest
			{
			    BackupLocation = BackupDir,
                DatabaseLocation = DataDir
			}, s => { }));

		}
	}
}