using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Mono.CSharp;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Raven.Backup;

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

			Assert.DoesNotThrow(() => DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => { }, defrag: false));

		}
	}
}