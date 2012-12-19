using System;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class IncrementalBackupRestore : RavenTest
	{
		private const string BackupDir = @".\BackupDatabase\";
		private DocumentDatabase db;

		public IncrementalBackupRestore()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
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

		[Fact]
		public void AfterIncrementalBackupRestoreCanReadDocument()
		{
			IOExtensions.DeleteDirectory(BackupDir);

			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			db.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Put("itamar", null, RavenJObject.Parse("{'email':'itamar@ayende.com'}"), new RavenJObject(), null);
			db.StartBackup(BackupDir, true, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

			DocumentDatabase.Restore(new RavenConfiguration
			{
				Settings =
				{
					{"Raven/Esent/CircularLog", "false"}
				}

			}, BackupDir, DataDir, s => { });

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir });
			
			var jObject = db.Get("ayende", null).ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
			jObject = db.Get("itamar", null).ToJson();
			Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));
		}

		[Fact]
		public void IncrementalBackupWithCircularLogThrows()
		{
			db.Dispose();
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			});

			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			Assert.Throws<InvalidOperationException>(() => db.StartBackup(BackupDir, true, new DatabaseDocument()));
		}
	}
}