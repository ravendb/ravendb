// -----------------------------------------------------------------------
//  <copyright file="PeriodicBackupTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Database.Bundles.PeriodicExports;
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
        public void CanSetupPeriodicExportWithVeryLargePeriods()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    var periodicExportTask = store.DocumentDatabase.StartupTasks.OfType<PeriodicExportTask>().FirstOrDefault();
                    var intervalMilliseconds = (long) TimeSpan.FromDays(50).TotalMilliseconds;
                    Assert.Equal(50, TimeSpan.FromMilliseconds(intervalMilliseconds).TotalDays); //sanity check
                    using (var session = store.OpenSession())
                    {
                        var periodicBackupSetup = new PeriodicExportSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = intervalMilliseconds,
                            IntervalMilliseconds = intervalMilliseconds
                        };
                        session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);
                        session.SaveChanges();
                    }

                    Assert.NotNull(periodicExportTask); //sanity check

                    Assert.Equal(50, periodicExportTask.IncrementalInterval.TotalDays);
                    Assert.Equal(50, periodicExportTask.FullBackupInterval.TotalDays);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void PeriodicExport_should_work_with_long_intervals()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    var periodicExportTask = store.DocumentDatabase.StartupTasks.OfType<PeriodicExportTask>().FirstOrDefault();

                    //get by reflection the maxTimerTimeoutInMilliseconds field
                    //this field is the maximum interval acceptable in .Net's threading timer
                    //if the requested export interval is bigger than this maximum interval, 
                    //a timer with maximum interval will be used several times until the interval cumulatively
                    //will be equal to requested interval
                    var maxTimerTimeoutInMillisecondsField = typeof(PeriodicExportTask)
                        .GetField("maxTimerTimeoutInMilliseconds",
                            BindingFlags.Instance | BindingFlags.NonPublic);

                    Assert.NotNull(maxTimerTimeoutInMillisecondsField); //sanity check, can fail here only in case of source code change
                    //that removes this field
                    maxTimerTimeoutInMillisecondsField.SetValue(periodicExportTask, 5);

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
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];
                    var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                    Assert.NotNull(fullBackupFilePath);

                    // import the full backup
                    var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath }).Wait();

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

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupToDirectory()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
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
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];
                    var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                    Assert.NotNull(fullBackupFilePath);

                    // import the full backup
                    var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath }).Wait();

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

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupToDirectory_MultipleBackups()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
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
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];
                    var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                    Assert.NotNull(fullBackupFilePath);

                    // import the full backup
                    var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath }).Wait();

                    // import the incremental backup
                    dataDumper.Options.Incremental = true;
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = actualBackupPath }).Wait();

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal("oren", session.Load<User>(1).Name);
                        Assert.Equal("ayende", session.Load<User>(2).Name);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupToDirectory_MultipleBackups_with_long_interval()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    var periodicExportTask = store.DocumentDatabase.StartupTasks.OfType<PeriodicExportTask>().FirstOrDefault();

                    //get by reflection the maxTimerTimeoutInMilliseconds field
                    //this field is the maximum interval acceptable in .Net's threading timer
                    //if the requested export interval is bigger than this maximum interval, 
                    //a timer with maximum interval will be used several times until the interval cumulatively
                    //will be equal to requested interval
                    var maxTimerTimeoutInMillisecondsField = typeof(PeriodicExportTask)
                        .GetField("maxTimerTimeoutInMilliseconds",
                            BindingFlags.Instance | BindingFlags.NonPublic);

                    Assert.NotNull(maxTimerTimeoutInMillisecondsField); //sanity check, can fail here only in case of source code change
                    //that removes this field
                    maxTimerTimeoutInMillisecondsField.SetValue(periodicExportTask, 5);

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

                    var etagForBackups = store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey).Etag;
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
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];
                    var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                    Assert.NotNull(fullBackupFilePath);

                    // import the full backup
                    var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath }).Wait();

                    // import the incremental backup
                    dataDumper.Options.Incremental = true;
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = actualBackupPath }).Wait();

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal("oren", session.Load<User>(1).Name);
                        Assert.Equal("ayende", session.Load<User>(2).Name);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }
    }
}
