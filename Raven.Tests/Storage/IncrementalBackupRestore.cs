using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
	public class IncrementalBackupRestore : TransactionalStorageTestBase
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

		[Theory]
        [PropertyData("Storages")]
        public void AfterIncrementalBackupRestoreCanReadDocument(string storageName)
		{
            InitializeDocumentDatabase(storageName);
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
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"}
	            }

			}, BackupDir, DataDir, s => { }, defrag: true);

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir });

		    var fetchedData = db.Get("ayende", null);
            Assert.NotNull(fetchedData);

		    var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Get("itamar", null);
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

            db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

            db.StartBackup(BackupDir, false, new DatabaseDocument());
            WaitForBackup(db, true);

            Thread.Sleep(TimeSpan.FromSeconds(1));

            db.Put("itamar", null, RavenJObject.Parse("{'email':'itamar@ayende.com'}"), new RavenJObject(), null);
            db.StartBackup(BackupDir, true, new DatabaseDocument());
            WaitForBackup(db, true);

            Thread.Sleep(TimeSpan.FromSeconds(1));

            db.Put("michael", null, RavenJObject.Parse("{'email':'michael.yarichuk@ayende.com'}"), new RavenJObject(), null);
            db.StartBackup(BackupDir, true, new DatabaseDocument());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            DocumentDatabase.Restore(new RavenConfiguration
            {
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"}
	            }

            }, BackupDir, DataDir, s => { }, defrag: true);

            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDir });

            var fetchedData = db.Get("ayende", null);
            Assert.NotNull(fetchedData);

            var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Get("itamar", null);
            Assert.NotNull(fetchedData);

            jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));

            fetchedData = db.Get("michael", null);
            Assert.NotNull(fetchedData);

            jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("michael.yarichuk@ayende.com", jObject.Value<string>("email"));

        }

        [Fact]
        public void IncrementalBackupWithCircularLogThrows()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
                RunInMemory = false,
                DefaultStorageTypeName = "esent",
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			});

			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			Assert.Throws<InvalidOperationException>(() => db.StartBackup(BackupDir, true, new DatabaseDocument()));
		}
	}
}