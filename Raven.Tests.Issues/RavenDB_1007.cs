// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1007.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1007_incremental_backup : RavenTest
	{
		private readonly string DataDir;
		private readonly string BackupDir;

		public RavenDB_1007_incremental_backup()
		{
			DataDir = NewDataPath("RavenDB-1007-IncrementalBackup");
			BackupDir = NewDataPath("RavenDB-1007-BackupDatabase");
		}

		[Theory]
        [PropertyData("Storages")]
		public void AfterFailedRestoreOfIndex_ShouldGenerateWarningAndResetIt(string storageName)
		{
			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,				
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
				DefaultStorageTypeName = storageName,
				Settings =
				{
					{Constants.Esent.CircularLog, "false"},
					{Constants.Voron.AllowIncrementalBackups, "true"}
				}
			}.Initialize(), null))
			{
				db.SpinBackgroundWorkers();
				db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

				db.Documents.Put("users/1", null, RavenJObject.Parse("{'Name':'Arek'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Documents.Put("users/2", null, RavenJObject.Parse("{'Name':'David'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				var databaseDocument = new DatabaseDocument();
				db.Maintenance.StartBackup(BackupDir, false, databaseDocument);
				WaitForBackup(db, true);

				db.Documents.Put("users/3", null, RavenJObject.Parse("{'Name':'Daniel'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				db.Maintenance.StartBackup(BackupDir, true, databaseDocument);
				WaitForBackup(db, true);

			}
			IOExtensions.DeleteDirectory(DataDir);

			var incrementalDirectories = Directory.GetDirectories(BackupDir, "Inc*");

			// delete 'index-files.required-for-index-restore' to make backup corrupted according to the reported error
			var combine = Directory.GetFiles(incrementalDirectories.First(), "index-files.required-for-index-restore",SearchOption.AllDirectories).First();
			File.Delete(combine);

			var sb = new StringBuilder();

		    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
		    {
		        BackupLocation = BackupDir,
		        DatabaseLocation = DataDir
		    }, s => sb.Append(s));

			Assert.Contains(
				"could not be restored. All already copied index files was deleted." +
				" Index will be recreated after launching Raven instance",
				sb.ToString());

			using (var db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir}, null))
			{				
				db.SpinBackgroundWorkers();
				QueryResult queryResult;
				do
				{					
					queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
					{
						Query = "Tag:[[Users]]",
						PageSize = 10
					}, CancellationToken.None);
				} while (queryResult.IsStale);
				Assert.Equal(3, queryResult.Results.Count);
			}
		}
	}

	public class RavenDB_1007_standard_backup : RavenTest
	{
		private readonly string DataDir;
		private readonly string BackupDir;

		public RavenDB_1007_standard_backup()
		{
			DataDir = NewDataPath("RavenDB-1007-StandardBackup");
			BackupDir = NewDataPath("RavenDB-1007-BackupDatabase");
		}

		[Theory]
		[PropertyData("Storages")]
		public void AfterFailedRestoreOfIndex_ShouldGenerateWarningAndResetIt(string storageName)
		{
			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DefaultStorageTypeName = storageName,
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			}, null))
			{
				db.SpinBackgroundWorkers();
				db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

				db.Documents.Put("users/1", null, RavenJObject.Parse("{'Name':'Arek'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Documents.Put("users/2", null, RavenJObject.Parse("{'Name':'David'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Documents.Put("users/3", null, RavenJObject.Parse("{'Name':'Daniel'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
				WaitForBackup(db, true);
			}
			IOExtensions.DeleteDirectory(DataDir);

			var path = Directory.GetFiles(BackupDir, "index-files.required-for-index-restore", SearchOption.AllDirectories).First();
			// lock file to simulate IOException when restore operation will try to copy this file
			using (var file = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
			{
				var sb = new StringBuilder();

			    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
			    {
			        BackupLocation = BackupDir,
			        DatabaseLocation = DataDir
				}, s => sb.Append(s));

				Assert.Contains(
					"could not be restored. All already copied index files was deleted." +
					" Index will be recreated after launching Raven instance",
					sb.ToString());
			}

			using (var db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir}, null))
			{
				db.SpinBackgroundWorkers();
				QueryResult queryResult;
				do
				{
					queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
					{
						Query = "Tag:[[Users]]",
						PageSize = 10
					}, CancellationToken.None);
				} while (queryResult.IsStale);
				Assert.Equal(3, queryResult.Results.Count);
			}
		}
	}
}