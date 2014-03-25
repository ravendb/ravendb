// -----------------------------------------------------------------------
//  <copyright file="RavenDB1369.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1369 : RavenTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        private string backupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-1369.Backup");
        private string dataDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-1369.Restore-Data");
        private string indexesDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-1369.Restore-Indexes");
        private string jouranlDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-1369.Restore-Journals");
        public RavenDB1369()
        {
            IOExtensions.DeleteDirectory(backupDir);
            IOExtensions.DeleteDirectory(dataDir);
            IOExtensions.DeleteDirectory(indexesDir);
            IOExtensions.DeleteDirectory(jouranlDir);
        }

        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory(backupDir);
            IOExtensions.DeleteDirectory(dataDir);
            IOExtensions.DeleteDirectory(indexesDir);
            IOExtensions.DeleteDirectory(jouranlDir);
        }


        [Fact]
        public void CanRestoreFullToMultipleLocationsEmbedded()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                using (var sesion = store.OpenSession())
                {
                    sesion.Store(new User { Name = "Regina" });
                    sesion.SaveChanges();
                }

                store.DocumentDatabase.Maintenance.StartBackup(backupDir, false, new DatabaseDocument());
                WaitForBackup(store.DocumentDatabase, true);
            }

            MaintenanceActions.Restore(new RavenConfiguration(), new RestoreRequest
            {
                BackupLocation = backupDir,
                DatabaseLocation = dataDir,
                DatabaseName = "foo",
                IndexesLocation = indexesDir,
                JournalsLocation = jouranlDir
            }, Console.WriteLine);

            using (var store = NewDocumentStore(runInMemory: false, configureStore: documentStore =>
            {
                documentStore.Configuration.DataDirectory = dataDir;
                documentStore.Configuration.IndexStoragePath = indexesDir;
                documentStore.Configuration.JournalsStoragePath = jouranlDir;
            }))
            {
                using (var sesion = store.OpenSession())
                {
                    Assert.Equal("Regina", sesion.Load<User>(1).Name);
                }

            }
        }

        [Fact]
        public void CanRestoreFullToMultipleLocationsToDifferentDatabase()
        {
            using (GetNewServer(runInMemory: false))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "DB1",
                    Settings =
                    {
                        {"Raven/DataDir", "~\\Databases\\db1"}
                    }
                });

                using (var sesion = store.OpenSession("DB1"))
                {
                    sesion.Store(new User { Name = "Regina" });
                    sesion.SaveChanges();
                }

                store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), "DB1");
                WaitForBackup(store.DatabaseCommands.ForDatabase("DB1"), true);

                store.DatabaseCommands.GlobalAdmin.StartRestore(new RestoreRequest
                {
                    BackupLocation = backupDir,
                    DatabaseLocation = dataDir,
                    IndexesLocation = indexesDir,
                    JournalsLocation = jouranlDir,
                    DatabaseName = "DB2"
                });

                WaitForRestore(store.DatabaseCommands);

                WaitForUserToContinueTheTest();

                using (var sesion = store.OpenSession("DB2"))
                {
                    Assert.Equal("Regina", sesion.Load<User>(1).Name);
                }
            }

        }
    }
}