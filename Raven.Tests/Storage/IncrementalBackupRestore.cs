using System;
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

namespace Raven.Tests.Storage
{
	public class IncrementalBackupRestore : RavenTest
	{
		private readonly string DataDir;
		private readonly string BackupDir;

		private DocumentDatabase db;

		public IncrementalBackupRestore()
		{
			BackupDir = NewDataPath("BackupDatabase");
			DataDir = NewDataPath("DataDirectory");
		}

	    private void InitializeDocumentDatabase(string storageName)
	    {
	        db = new DocumentDatabase(new RavenConfiguration
	        {
                DefaultStorageTypeName = storageName,
	            DataDirectory = DataDir,
                RunInMemory = false,
	            RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
	            Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"},
					{"Raven/Voron/AllowIncrementalBackups", "true"}
	            }
	        }.Initialize(), null);
	        db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
	    }

	    public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Theory]
        [PropertyData("Storages")]
        public void AfterIncrementalBackupRestoreCanReadDocument(string storageName)
		{
            InitializeDocumentDatabase(storageName);
			IOExtensions.DeleteDirectory(BackupDir);

			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Documents.Put("itamar", null, RavenJObject.Parse("{'email':'itamar@ayende.com'}"), new RavenJObject(), null);
			db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
			WaitForBackup(db, true);

			db.Dispose();
			IOExtensions.DeleteDirectory(DataDir);

			MaintenanceActions.Restore(new RavenConfiguration
			{
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"},
					{"Raven/Voron/AllowIncrementalBackups", "true"}
	            }

			}, new DatabaseRestoreRequest
			{
			    BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                Defrag = true
			}, s => { });

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);

		    var fetchedData = db.Documents.Get("ayende", null);
            Assert.NotNull(fetchedData);

		    var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Documents.Get("itamar", null);
            Assert.NotNull(fetchedData);
            
            jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));
		}

        [Theory]
        [PropertyData("Storages")]
        public void AfterMultipleIncrementalBackupRestoreCanReadDocument(string storageName)
        {
            InitializeDocumentDatabase(storageName);
            IOExtensions.DeleteDirectory(BackupDir);

            db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
            WaitForBackup(db, true);

            Thread.Sleep(TimeSpan.FromSeconds(1));

            db.Documents.Put("itamar", null, RavenJObject.Parse("{'email':'itamar@ayende.com'}"), new RavenJObject(), null);
            db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
            WaitForBackup(db, true);

            Thread.Sleep(TimeSpan.FromSeconds(1));

            db.Documents.Put("michael", null, RavenJObject.Parse("{'email':'michael.yarichuk@ayende.com'}"), new RavenJObject(), null);
            db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            MaintenanceActions.Restore(new RavenConfiguration
            {
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"},
					{"Raven/Voron/AllowIncrementalBackups", "true"}
	            }

            }, new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                Defrag = true
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir }, null);

            var fetchedData = db.Documents.Get("ayende", null);
            Assert.NotNull(fetchedData);

            var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Documents.Get("itamar", null);
            Assert.NotNull(fetchedData);

            jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Documents.Get("michael", null);
            Assert.NotNull(fetchedData);

            jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("michael.yarichuk@ayende.com", jObject.Value<string>("email"));

        }

		[Theory]
		[PropertyData("Storages")]
		public void IncrementalBackupWithCircularLogOrVoronIncrementalBackupsNotEnabledThrows(string storageName)
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
                RunInMemory = false,
				DefaultStorageTypeName = storageName,
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			}, null);

			db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		
			db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			Assert.Throws<InvalidOperationException>(() => db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument()));
		}
	}
}