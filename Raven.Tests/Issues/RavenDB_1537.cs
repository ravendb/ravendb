// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1537.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Tests.Bundles.PeriodicBackups;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
    public class RavenDB_1537 : RavenTest
    {

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
        }
        public class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanFullBackupToDirectory()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicBackupSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 500
                        };
                        session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                        session.SaveChanges();

                    }
                    SpinWait.SpinUntil(() =>
                    {
                        var jsonDocument = store.DatabaseCommands.Get(PeriodicBackupStatus.RavenDocumentKey);
                        if (jsonDocument == null)
                            return false;
                        var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
                        return periodicBackupStatus.LastFullBackup != new DateTime();
                    });

                }
                using (var store = NewDocumentStore())
                {
                    var dataDumper = new DataDumper(store.DocumentDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions
                    {
                        FromFile = Directory.GetFiles(Path.GetFullPath(backupPath))
                          .Where(file => ".ravendb-full-dump".Equals(Path.GetExtension(file), StringComparison.InvariantCultureIgnoreCase))
                          .OrderBy(File.GetLastWriteTimeUtc).First()

                    }, new SmugglerOptions
                    {
                        Incremental = false
                    }).Wait();

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal("oren", session.Load<User>(1).Name);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }


        }

        [Fact]
        public void CanFullBackupToDirectory_MultipleBackups()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicBackupSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 500
                        };
                        session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);

                    // we have first backup finished here, now insert second object

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "ayende" });
                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                }


                var files = Directory.GetFiles(Path.GetFullPath(backupPath))
                                     .Where(
                                         f =>
                                         ".ravendb-full-dump".Equals(Path.GetExtension(f),
                                                                     StringComparison.InvariantCultureIgnoreCase))
                                     .OrderBy(File.GetLastWriteTimeUtc).ToList(); 
                AssertUsersCountInBackup(1, files.First());
                AssertUsersCountInBackup(2, files.Last());


            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }


        private DateTime? GetLastFullBackupTime(IDocumentStore store)
        {
            var jsonDocument =
                    store.DatabaseCommands.Get(PeriodicBackupStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return null;
            var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
            return periodicBackupStatus.LastFullBackup;
        }
        private void WaitForNextFullBackup(IDocumentStore store)
        {
            var lastFullBackup = DateTime.MinValue;

            SpinWait.SpinUntil(() =>
            {
                var backupTime = GetLastFullBackupTime(store);
                if (backupTime.HasValue == false)
                    return false;
                if (lastFullBackup == DateTime.MinValue)
                {
                    lastFullBackup = backupTime.Value;
                    return false;
                }
                return lastFullBackup != backupTime;
            }, 5000);
        }

        private void AssertUsersCountInBackup(int expectedNumerOfUsers, string file)
        {
                using (var store = NewDocumentStore())
                {
                    var dataDumper = new DataDumper(store.DocumentDatabase);

                    dataDumper.ImportData(new SmugglerImportOptions
                    {
                        FromFile = file

                    }, new SmugglerOptions
                    {
                        Incremental = false
                    }).Wait();

                    WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(expectedNumerOfUsers, session.Query<User>().Count());
                    }
                }
        }
    }
}