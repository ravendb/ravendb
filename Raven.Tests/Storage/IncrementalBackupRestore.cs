using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class IncrementalBackupRestore : AbstractDocumentStorageTest
	{
		private DocumentDatabase db;

		public IncrementalBackupRestore()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
				Settings =
					{
						{"Raven/Esent/CircularLog", "false"}
					}
			});
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		private static void DeleteIfExists(string DirectoryName)
		{
			string directoryFullName = null;

			if (Path.IsPathRooted(DirectoryName) == false)
				directoryFullName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DirectoryName);
			else
				directoryFullName = DirectoryName;

			IOExtensions.DeleteDirectory(directoryFullName);
		}

		private void WaitForBackup(bool checkError)
		{
			while (true)
			{
				var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					break;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					if (checkError)
					{
						var firstOrDefault = backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
						if(firstOrDefault != null)
							Assert.False(true, firstOrDefault.Message);
					}

					return;
				}
				Thread.Sleep(50);
			}
		}

		[Fact]
		public void AfterIncrementalBackupRestoreCanReadDocument()
		{
			DeleteIfExists("raven.db.test.backup"); // for full backups, we can't have anything in the target dir

			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			db.StartBackup("raven.db.test.backup", false);
			WaitForBackup(true);

			db.Put("itamar", null, RavenJObject.Parse("{'email':'itamar@ayende.com'}"), new RavenJObject(), null);
			db.StartBackup("raven.db.test.backup", true);
			WaitForBackup(true);

			db.Dispose();

			DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration
			{
				Settings =
					{
						{"Raven/Esent/CircularLog", "false"}
					}

			}, "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });

			var jObject = db.Get("ayende", null).ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
			jObject = db.Get("itamar", null).ToJson();
			Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));
		}
	}
}