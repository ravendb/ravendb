// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2779.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Tests.Helpers;

using Xunit;

using Raven.Abstractions.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_2779 : RavenTestBase
    {
        private readonly string backupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-2779.Backup");
        private readonly string dataDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-2779.Restore-Data");
        private readonly string indexesDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-2779.Restore-Indexes");
        private readonly string jouranlDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB-2779.Restore-Journals");

        public RavenDB_2779()
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
        public void WaitForResultsInRestoreShouldntTakeTooLong()
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
                    sesion.Store(new RavenDB1369.User { Name = "Regina" });
                    sesion.SaveChanges();
                }

                store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), false, "DB1").WaitForCompletion();

                var restoreOpertion = store.DatabaseCommands.GlobalAdmin.StartRestore(new DatabaseRestoreRequest
                                                                                {
                                                                                    BackupLocation = backupDir,
                                                                                    DatabaseLocation = dataDir,
                                                                                    IndexesLocation = indexesDir,
                                                                                    JournalsLocation = jouranlDir,
                                                                                    DatabaseName = "DB2"
                                                                                });

                restoreOpertion.WaitForCompletion();

                using (var sesion = store.OpenSession("DB2"))
                {
                    Assert.Equal("Regina", sesion.Load<RavenDB1369.User>(1).Name);
                }
            }
        }

        [Fact]
        public void WaitForCompletionBehaveCorrectlyWhenRestoreFails()
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
                    sesion.Store(new RavenDB1369.User { Name = "Regina" });
                    sesion.SaveChanges();
                }

                store.DatabaseCommands.GlobalAdmin.StartBackup(backupDir, new DatabaseDocument(), false, "DB1").WaitForCompletion();

                var operation = store.DatabaseCommands.GlobalAdmin.StartRestore(new DatabaseRestoreRequest
                {
                    BackupLocation = backupDir,
                    DatabaseLocation = dataDir,
                    IndexesLocation = indexesDir,
                    JournalsLocation = jouranlDir,
                    DatabaseName = "DB2"
                });

                // delete backup to make it corrupted
                File.Delete(Path.Combine(backupDir, "RavenDB.Voron.Backup"));

                Assert.Throws<InvalidOperationException>(() => operation.WaitForCompletion());
            }
        }
    }
}
