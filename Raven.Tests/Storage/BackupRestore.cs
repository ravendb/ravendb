//-----------------------------------------------------------------------
// <copyright file="BackupRestore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

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
			}, null);
			db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void AfterBackupRestoreCanReadDocument()
		{
			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);
			
			db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

		    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
		    {
		        BackupLocation = BackupDir,
		        DatabaseLocation = DataDir,
		        Defrag = true
		    }, s => { });

			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDir}, null);

			var document = db.Documents.Get("ayende", null);
			Assert.NotNull(document);

			var jObject = document.ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
		}


		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedAfterRestore()
		{
			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

			db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

		    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
		    {
		        BackupLocation = BackupDir,
		        DatabaseLocation = DataDir
		    }, s => { });

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);
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
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedBeforeRestore()
		{
			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
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
			Assert.Equal(1, queryResult.Results.Count);

			db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

		    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
		    {
		        BackupLocation = BackupDir,
		        DatabaseLocation = DataDir,
                Defrag = true
		    }, s => { });

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);

			queryResult = db.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				Query = "Tag:[[Users]]",
				PageSize = 10
			}, CancellationToken.None);
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterFailedBackupRestoreCanDetectError()
		{
			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
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
			Assert.Equal(1, queryResult.Results.Count);

			File.WriteAllText("raven.db.test.backup.txt", "Sabotage!");
			db.Maintenance.StartBackup("raven.db.test.backup.txt", false, new DatabaseDocument());
			WaitForBackup(db, false);

		    var condition = GetStateOfLastStatusMessage().Severity == BackupStatus.BackupMessageSeverity.Error;
		    Assert.True(condition);
		}

		private BackupStatus.BackupMessage GetStateOfLastStatusMessage()
		{
			JsonDocument jsonDocument = db.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
			var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
			return backupStatus.Messages.Last();
		}
	}
}