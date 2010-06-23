using System;
using System.IO;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Data;
using Raven.Database.Json;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class BackupRestore : AbstractDocumentStorageTest
	{
		private DocumentDatabase db;

		public BackupRestore()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

        private void DeleteIfExists(string DirectoryName)
        {
            string directoryFullName = null;

            if (Path.IsPathRooted(DirectoryName) == false)
                directoryFullName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DirectoryName);
            else
                directoryFullName = DirectoryName;

            if (Directory.Exists(directoryFullName))
                Directory.Delete(directoryFullName, true);
        }
		
		[Fact]
		public void AfterBackupRestoreCanReadDocument()
		{
			db.Put("ayende", null, JObject.Parse("{'email':'ayende@ayende.com'}"), new JObject(), null);

			db.StartBackup("raven.db.test.backup");
			WaitForBackup();

			db.Dispose();

            DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent"});

			var jObject = db.Get("ayende", null).ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedAfterRestore()
		{
			db.Put("ayende", null, JObject.Parse("{'email':'ayende@ayende.com'}"), JObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

			db.StartBackup("raven.db.test.backup");
			WaitForBackup();

			db.Dispose();

            DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });
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
			Assert.Equal(1, queryResult.Results.Length);
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedBeforeRestore()
		{
			db.Put("ayende", null, JObject.Parse("{'email':'ayende@ayende.com'}"), JObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
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

			db.StartBackup("raven.db.test.backup");
			WaitForBackup();

			db.Dispose();

            DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });
			queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				Query = "Tag:[[Users]]",
				PageSize = 10
			});
			Assert.Equal(1, queryResult.Results.Length);
		}

		private void WaitForBackup()
		{
			while (true)
			{
				var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					break;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					var message = backupStatus.Messages.LastOrDefault(x => x.Message.Contains("Failed"));

					if (message == null)
						return;
					throw new InvalidOperationException(message.Message);
				}
				Thread.Sleep(50);
			}
		}
	}
}