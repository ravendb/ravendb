using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_10635 : RavenTestBase
    {
        private readonly string DataDir;
        private readonly string BackupDir;

        private DocumentDatabase db;

        public RavenDB_10635()
        {
            BackupDir = NewDataPath("BackupDatabase");
            DataDir = NewDataPath("DataDirectory");
        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Theory]
        [PropertyData("Storages")]
        public void Full_backup_then_restore_should_work_with_custom_journal_folder(string storageName)
        {
            InitializeDocumentDatabase(storageName);
            IOExtensions.DeleteDirectory(BackupDir);

            //generate some journal entries
            db.Documents.Put("Foo", null, RavenJObject.Parse("{'email':'foo@bar.com'}"), new RavenJObject(), null);
            db.Documents.Put("Foo", null, RavenJObject.Parse("{'email':'foo@bar2.com'}"), new RavenJObject(), null);
            db.Documents.Put("Foo", null, RavenJObject.Parse("{'email':'foo@bar3.com'}"), new RavenJObject(), null);

            db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState());
            WaitForBackup(db, true);

            db.Dispose();
            IOExtensions.DeleteDirectory(DataDir);

            var journalsCustomPath = Path.Combine(DataDir,"Journals");
            MaintenanceActions.Restore(new RavenConfiguration
            {
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false          
            }, new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = DataDir,
                JournalsLocation = journalsCustomPath,
                Defrag = true
            }, s => { });

            db = new DocumentDatabase(new RavenConfiguration
            {
                DataDirectory = DataDir,
                RunInMemory = false,
                Settings =
                {
                    {"Raven/TransactionJournalsPath", journalsCustomPath }
                }
            }, null);

            var fetchedData = db.Documents.Get("Foo", null);
            Assert.NotNull(fetchedData);

            var jObject = fetchedData.ToJson();
            Assert.NotNull(jObject);
            Assert.Equal("foo@bar3.com", jObject.Value<string>("email"));

            db.Dispose();
        }

        private void InitializeDocumentDatabase(string storageName)
        {
            db = new DocumentDatabase(new RavenConfiguration
            {
                DefaultStorageTypeName = storageName,
                DataDirectory = DataDir,
                RunInMemory = false,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false              
            }, null);
            db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
        }

    }
}
