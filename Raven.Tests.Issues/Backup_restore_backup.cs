// -----------------------------------------------------------------------
//  <copyright file="Backup_restore_backup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class Backup_restore_backup : RavenTest
    {
        private const string BackupDir = @".\BackupDatabase\";
        private int _testDataCounter = 0; 

        public Backup_restore_backup()
        {
            IOExtensions.DeleteDirectory(BackupDir);
        }

        [Fact(Timeout = 60000)]
        public void Full_and_incremental_backup_restore_should_work()
        {
            string dataDirectory;
            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                PutSomeData(store);
                PutSomeData(store);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);               
                
                PutSomeData(store);
                
                var indexDefinitionsFolder = Path.Combine(store.SystemDatabase.Configuration.DataDirectory, "IndexDefinitions");
                if (!Directory.Exists(indexDefinitionsFolder))
                    Directory.CreateDirectory(indexDefinitionsFolder);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);

                PutSomeData(store);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);
                dataDirectory = store.DataDirectory;
            }
            
            IOExtensions.DeleteDirectory(dataDirectory);
            Directory.CreateDirectory(dataDirectory);
            
            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = dataDirectory,
                Defrag = true
            }, s => { });
            
            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                var ids = Enumerable.Range(0, _testDataCounter).Select(suffix => "users/" + suffix).ToArray();
                var names = Enumerable.Range(0, _testDataCounter).Select(suffix => "John Dow " + suffix).ToArray();
                using (var session = store.OpenSession())
                {
                    var docs = session.Load<User>(ids);
                    Assert.DoesNotContain(null,docs);
                    foreach (var name in names)
                    {
                        Assert.True(docs.Any(user => user.Name.Equals(name)));
                    }
                }
            }
        }

        [Fact(Timeout = 60000)]
        public void Should_not_cause_missing_journal_exception()
        {
            string dataDirectory;
            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                PutSomeData(store);

                var indexDefinitionsFolder = Path.Combine(store.SystemDatabase.Configuration.DataDirectory, "IndexDefinitions");
                if (!Directory.Exists(indexDefinitionsFolder))
                    Directory.CreateDirectory(indexDefinitionsFolder);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);

                PutSomeData(store);

                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);
                dataDirectory = store.DataDirectory;
            }
            
            IOExtensions.DeleteDirectory(dataDirectory);
            Directory.CreateDirectory(dataDirectory);
            
            MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
            {
                BackupLocation = BackupDir,
                DatabaseLocation = dataDirectory,
                Defrag = true
            }, s => { });
            
            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                Assert.DoesNotThrow(() => store.SystemDatabase.Maintenance.StartBackup(BackupDir + "B", true, new DatabaseDocument(), new ResourceBackupState()));
                WaitForBackup(store.SystemDatabase, true);                
            }
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/Esent/CircularLog"] = "false";
            configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "true";
            configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
            configuration.Initialize();
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory(BackupDir);
            IOExtensions.DeleteDirectory(BackupDir + "B");
            base.Dispose();
        }

        
        private void PutSomeData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    Name = "John Dow " + _testDataCounter
                },"users/" + _testDataCounter);
                session.SaveChanges();
                
                Interlocked.Increment(ref _testDataCounter);
            }
        }
    }
}