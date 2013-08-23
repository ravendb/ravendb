//-----------------------------------------------------------------------
// <copyright file="BackupRestore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class BackupRestore : RavenTest
	{
		private readonly string DataDir;
		private readonly string BackupDir;
		private DocumentDatabase db;

		public BackupRestore()
		{
			BackupDir = NewDataPath("BackupDatabase");
			DataDir = NewDataPath("DataDirectory");

			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
			});
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void AfterBackupRestoreCanReadDocument()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			db.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

			DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => { }, defrag: true);

			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir});

			var jObject = db.Get("ayende", null).ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
		}


		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedAfterRestore()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

			db.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

			DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => { }, defrag: true);

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir });
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
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedBeforeRestore()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
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
			Assert.Equal(1, queryResult.Results.Count);

			db.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

			DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => { }, defrag: true);

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir });

			queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				Query = "Tag:[[Users]]",
				PageSize = 10
			});
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterFailedBackupRestoreCanDetectError()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
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
			Assert.Equal(1, queryResult.Results.Count);

			File.WriteAllText("raven.db.test.backup.txt", "Sabotage!");
			db.StartBackup("raven.db.test.backup.txt", false, new DatabaseDocument());
			WaitForBackup(db, false);

			Assert.True(GetStateOfLastStatusMessage().Severity == BackupStatus.BackupMessageSeverity.Error);
		}

		private BackupStatus.BackupMessage GetStateOfLastStatusMessage()
		{
			JsonDocument jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
			var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
			return backupStatus.Messages.OrderByDescending(m => m.Timestamp).First();
		}
	}
}