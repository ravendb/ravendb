using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Storage;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests
{
    public class NonIncrementalBackupRestoreTest : TransactionalStorageTestBase
    {
		private readonly string DataDir;
		private readonly string BackupDir;

		private DocumentDatabase db;

        public NonIncrementalBackupRestoreTest()
		{
			BackupDir = NewDataPath("BackupDatabase");
			DataDir = NewDataPath("DataDirectory");
		}

        public override void Dispose()
		{
			db.Dispose();
            base.Dispose();
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
	        }, null);
	        db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
	    }

        [Theory]
        [PropertyData("Storages")]
        public void NonIncrementalBackup_Restore_CanReadDocument(string storageName)
        {
            InitializeDocumentDatabase(storageName);
            IOExtensions.DeleteDirectory(BackupDir);

            db.Documents.Put("Foo", null, RavenJObject.Parse("{'email':'foo@bar.com'}"), new RavenJObject(), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
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

            var fetchedData = db.Documents.Get("Foo", null);
            Assert.NotNull(fetchedData);

            var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("foo@bar.com", jObject.Value<string>("email"));

            db.Dispose();
        }

		[Theory]
		[PropertyData("Storages")]
        public void NonIncrementalBackup_Restore_DataDirectoryAlreadyExists_ExceptionThrown(string storageName)
        {
            InitializeDocumentDatabase(storageName);
            IOExtensions.DeleteDirectory(BackupDir);

            db.Documents.Put("Foo", null, RavenJObject.Parse("{'email':'foo@bar.com'}"), new RavenJObject(), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument());
            WaitForBackup(db, true);

            db.Dispose();

            //data directiory still exists --> should fail to restore backup
            Assert.Throws<IOException>(() => 
                MaintenanceActions.Restore(new RavenConfiguration
                {
                    DefaultStorageTypeName = storageName,
                    DataDirectory = DataDir,
                    RunInMemory = false,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                    Settings =
                    {
                        {"Raven/Esent/CircularLog", "false"}
                    }

                }, new DatabaseRestoreRequest
                {
                    BackupLocation = BackupDir,
                    DatabaseLocation = DataDir,
                    Defrag = true
                }, s => { }));
        }
        
    }
}
