// -----------------------------------------------------------------------
//  <copyright file="PeriodicBackupTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.PeriodicExports
{
    public class PeriodicBackupTests : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
        }
        public class User
        {
            public string Name { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupToDirectory()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    var periodicBackupSetup = new PeriodicExportSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 25
                    };
                    session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                    session.SaveChanges();

                }
                SpinWait.SpinUntil(() => store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey) != null, 10000);

            }

            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Equal("oren", session.Load<User>(1).Name);
                }
            }
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupToDirectory_MultipleBackups()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    var periodicBackupSetup = new PeriodicExportSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 25
                    };
                    session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                    session.SaveChanges();

                }
                SpinWait.SpinUntil(() =>
                {
                    var jsonDocument = store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey);
                    if (jsonDocument == null)
                        return false;
                    var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                    return periodicBackupStatus.LastDocsEtag != Etag.Empty && periodicBackupStatus.LastDocsEtag != null;
                });

                var etagForBackups= store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey).Etag;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "ayende" });
                    session.SaveChanges();
                }
                SpinWait.SpinUntil(() =>
                     store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey).Etag != etagForBackups);

            }

            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = {Incremental = true}};
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Equal("oren", session.Load<User>(1).Name);
                    Assert.Equal("ayende", session.Load<User>(2).Name);
                }
            }
            IOExtensions.DeleteDirectory(backupPath);
        }
    }
}
