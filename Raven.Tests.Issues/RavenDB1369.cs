// -----------------------------------------------------------------------
//  <copyright file="RavenDB1369.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1369 : RavenTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        public class User_ByName : AbstractIndexCreationTask<User>
        {
            public User_ByName()
            {
                Map = users =>
                    from user in users
                    select new { user.Name };
            }
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
        public void CanRestoreFullToMultipleLocationsEmbeddedWithIndexing()
        {
            string storage;
            using (var store = NewDocumentStore(runInMemory: false))
            {
                storage = store.Configuration.DefaultStorageTypeName;

                new User_ByName().Execute(store);

                using (var sesion = store.OpenSession())
                {
                    sesion.Store(new User { Name = "Regina" });
                    sesion.SaveChanges();
                }

                WaitForIndexing(store);

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

            var ravenConfiguration = new RavenConfiguration
            {
                DefaultStorageTypeName = storage,
                DataDirectory = dataDir,
                JournalsStoragePath = jouranlDir,
                IndexStoragePath = indexesDir
            };

            using (var db = new DocumentDatabase(ravenConfiguration))
            {
                //db.SpinBackgroundWorkers(); -- indexing disabled here

                var q = db.Queries.Query("User/ByName", new IndexQuery(), CancellationToken.None);
                Assert.Equal(1, q.TotalResults); // we read the indexed results from the restored data
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

                store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), false, "DB1");
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

                using (var sesion = store.OpenSession("DB2"))
                {
                    Assert.Equal("Regina", sesion.Load<User>(1).Name);
                }
            }

        }

        [Fact]
        public void CanRestoreIncrementalToMultipleLocationsToDifferentDatabase()
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
                        {"Raven/DataDir", "~\\Databases\\db1"},
                        {"Raven/Esent/CircularLog", "false"},
                        {"Raven/Voron/AllowIncrementalBackups", "true"}
                    }
                });

                for (int i = 0; i < 10; i++)
                {
                    using (var sesion = store.OpenSession("DB1"))
                    {
                        sesion.Store(new User { Name = "User " + i });
                        sesion.SaveChanges();
                    }

                    store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), true, "DB1");
                    WaitForBackup(store.DatabaseCommands.ForDatabase("DB1"), true);
					
					Thread.Sleep(1000); // incremental tag has seconds precision
                }

                store.DatabaseCommands.GlobalAdmin.StartRestore(new RestoreRequest
                {
                    BackupLocation = backupDir,
                    DatabaseLocation = dataDir,
                    IndexesLocation = indexesDir,
                    JournalsLocation = jouranlDir,
                    DatabaseName = "DB2"
                });

                WaitForRestore(store.DatabaseCommands);

                using (var sesion = store.OpenSession("DB2"))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var load = sesion.Load<User>(i+1);
                        if (load == null)
                        {
                            Debugger.Launch();
                            throw new InvalidOperationException("Cannot find user " + (i+1));
                        }
                        Assert.Equal("User " + i, load.Name);
                    }
                }
            }

        }

        [Fact]
        public void CanRestoreFullToMultipleLocationsToDifferentDatabaseWithIndexes()
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

                new User_ByName().Execute(store.DatabaseCommands.ForDatabase("DB1"), store.Conventions);

                store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), false, "DB1");
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
                Assert.NotNull(store.DatabaseCommands.ForDatabase("DB2").GetIndex("User/ByName"));
            }
        }
    }
}