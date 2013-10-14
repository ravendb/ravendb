// -----------------------------------------------------------------------
//  <copyright file="RavenDB_865.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_865 : RavenTest
	{
		private const string RestoredDatabaseName = "Database-865-Restore";

		[Fact]
		public async Task Restore_operation_works_async()
		{
			string backupDir = NewDataPath("BackupDatabase");
			string restoreDir = NewDataPath("RestoredDatabase");

			using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage:"esent"))
			{
				store.DatabaseCommands.Put("keys/1", null, new RavenJObject { { "Key", 1 } }, new RavenJObject());

				await store.AsyncDatabaseCommands.Admin.StartBackupAsync(backupDir, new DatabaseDocument());

				WaitForBackup(store.DatabaseCommands, true);

				// restore as a new database
				await store.AsyncDatabaseCommands.Admin.StartRestoreAsync(backupDir, restoreDir, RestoredDatabaseName);

				// get restore status and wait for finish
				WaitForRestore(store.DatabaseCommands);
				WaitForDocument(store.DatabaseCommands, "Raven/Databases/" + RestoredDatabaseName);

				Assert.Equal(1, store.DatabaseCommands.ForDatabase(RestoredDatabaseName).Get("keys/1").DataAsJson.Value<int>("Key"));
			}
		}
	}
}
