// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1007.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Bundles.MoreLikeThis;
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
			DataDir = NewDataPath("IncrementalBackup");
			BackupDir = NewDataPath("BackupDatabase");
		}

		[Theory]
		[InlineData("esent")]
		//[InlineData("voron")] //TODO : investigate Voron issue --> access violation exception when this runs
		public void AfterFailedRestoreOfIndex_ShouldGenerateWarningAndResetIt(string storageName)
		{
			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,				
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
				DefaultStorageTypeName = storageName,
				Settings =
				{
					{"Raven/Esent/CircularLog", "false"},
					{"Raven/Voron/AllowIncrementalBackups", "true"}
				}
			}))
			{
				db.SpinBackgroundWorkers();
				db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

				db.Put("users/1", null, RavenJObject.Parse("{'Name':'Arek'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Put("users/2", null, RavenJObject.Parse("{'Name':'David'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				var databaseDocument = new DatabaseDocument();
				db.StartBackup(BackupDir, false, databaseDocument);
				WaitForBackup(db, true);

				db.Put("users/3", null, RavenJObject.Parse("{'Name':'Daniel'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				db.StartBackup(BackupDir, true, databaseDocument);
				WaitForBackup(db, true);

			}
			IOExtensions.DeleteDirectory(DataDir);

			var incrementalDirectories = Directory.GetDirectories(BackupDir, "Inc*");

			// delete 'index-files.required-for-index-restore' to make backup corrupted according to the reported error
			var combine = Directory.GetFiles(incrementalDirectories.First(), "index-files.required-for-index-restore",SearchOption.AllDirectories).First();
			File.Delete(combine);

			var sb = new StringBuilder();

			DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => sb.Append(s), defrag: true);

			Assert.Contains(
				"could not be restored. All already copied index files was deleted." +
				" Index will be recreated after launching Raven instance",
				sb.ToString());

			using (var db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir}))
			{				
				db.SpinBackgroundWorkers();
				QueryResult queryResult;
				do
				{					
					queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
					{
						Query = "Tag:[[Users]]",
						PageSize = 10
					});
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
			DataDir = NewDataPath("IncrementalBackup");
			BackupDir = NewDataPath("BackupDatabase");
		}

		[Theory]
		[InlineData("esent")]
		//[InlineData("voron")] 
		public void AfterFailedRestoreOfIndex_ShouldGenerateWarningAndResetIt(string storageName)
		{
			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DefaultStorageTypeName = storageName,
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			}))
			{
				db.SpinBackgroundWorkers();
				db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

				db.Put("users/1", null, RavenJObject.Parse("{'Name':'Arek'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Put("users/2", null, RavenJObject.Parse("{'Name':'David'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Put("users/3", null, RavenJObject.Parse("{'Name':'Daniel'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				WaitForIndexing(db);

				db.StartBackup(BackupDir, false, new DatabaseDocument());
				WaitForBackup(db, true);
			}
			IOExtensions.DeleteDirectory(DataDir);

			var path = Directory.GetFiles(BackupDir, "index-files.required-for-index-restore", SearchOption.AllDirectories).First();
			// lock file to simulate IOException when restore operation will try to copy this file
			using (var file = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
			{
				var sb = new StringBuilder();

				DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => sb.Append(s), defrag: true);

				Assert.Contains(
					"could not be restored. All already copied index files was deleted." +
					" Index will be recreated after launching Raven instance",
					sb.ToString());
			}

			using (var db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir}))
			{
				db.SpinBackgroundWorkers();
				QueryResult queryResult;
				do
				{
					queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
					{
						Query = "Tag:[[Users]]",
						PageSize = 10
					});
				} while (queryResult.IsStale);
				Assert.Equal(3, queryResult.Results.Count);
			}
		}
	}
}