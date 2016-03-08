//-----------------------------------------------------------------------
// <copyright file="BackupRestore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
    public class BackupRestoreCancelation : RavenTest
    {
        private readonly string DataDir;
        private readonly string BackupDir;
        private DocumentDatabase db;

        public BackupRestoreCancelation()
        {
            BackupDir = NewDataPath("BackupDatabase");
            DataDir = NewDataPath("DataDirectory");
        }

        [Theory]
        [PropertyData("Storages")]
        public void CannotRestoreFailedBackup(string storageName)
        {
            var config = new RavenConfiguration
            {
                DataDirectory = DataDir,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                DefaultStorageTypeName = storageName,
                Settings =
                {
                    {Constants.Esent.CircularLog, "false"},
                    {Constants.Voron.AllowIncrementalBackups, "true"}
                }
            };

            config.Storage.Voron.AllowIncrementalBackups = true;

            using (db = new DocumentDatabase(config, null))
            {
                db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

                db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

                // purpose of doing first backup is to have Database.Document in target directory
                // we cancel second backup, when file is already in place
                db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState(), CancellationToken.None);
                WaitForBackup(db, true);

                db.Documents.Put("marcin", null, RavenJObject.Parse("{'email':'marcin@ayende.com'}"), new RavenJObject(), null);

                using (var cts = new CancellationTokenSource())
                {
                    cts.Cancel();
                    db.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument(), new ResourceBackupState(), cts.Token);
                    var ex = Assert.Throws<Exception>(() => WaitForBackup(db, true));
                    Assert.Contains("Backup was canceled", ex.Message);
                }

                db.Dispose();
                IOExtensions.DeleteDirectory(DataDir);

                var restoreEx = Assert.Throws<InvalidOperationException>(() =>
                {
                    MaintenanceActions.Restore(new RavenConfiguration(), new DatabaseRestoreRequest
                    {
                        BackupLocation = BackupDir,
                        DatabaseLocation = DataDir,
                        Defrag = true
                    }, s => { });
                });

                Assert.Equal("Backup failure marker was detected. Unable to restore from given directory.", restoreEx.Message);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanCancelBackupDuringDataBackup(string storageName)
        {
            var config = new RavenConfiguration
            {
                DataDirectory = DataDir,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                DefaultStorageTypeName = storageName,
            };

            using (db = new DocumentDatabase(config, null))
            {
                db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

                db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

                using (var cts = new CancellationTokenSource())
                {
                    var state = new ThrowingResourceBackupState("Finished indexes backup. Executing data backup", cts);
                    db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), state, cts.Token);
                    var ex = Assert.Throws<Exception>(() => WaitForBackup(db, true));
                    Assert.Contains("Backup was canceled", ex.Message);

                    Assert.False(File.Exists(Path.Combine(BackupDir, Constants.DatabaseDocumentFilename)));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void MarkBackupAsFailedWhenShuttingDownDatabase(string storageName)
        {
            var config = new RavenConfiguration
            {
                DataDirectory = DataDir,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                DefaultStorageTypeName = storageName
            };

            using (db = new DocumentDatabase(config, null))
            {
                db.Indexes.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

                db.Documents.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

                using (var cts = new CancellationTokenSource())
                {
                    var task = db.Maintenance.StartBackup(BackupDir, false, new DatabaseDocument(), new ResourceBackupState(), cts.Token);

                    db.Dispose();

                    task.Wait();
                }

                Assert.True(File.Exists(Path.Combine(BackupDir, Constants.BackupFailureMarker)));
            }
        }
    }


    public class ThrowingResourceBackupState : ResourceBackupState
    {
        private readonly string progressSubstringToCancelToken;

        private readonly CancellationTokenSource cts;

        public ThrowingResourceBackupState(string progressSubstringToCancelToken, CancellationTokenSource cts)
        {
            this.progressSubstringToCancelToken = progressSubstringToCancelToken;
            this.cts = cts;
        }

        public override void MarkProgress(string progress)
        {
            base.MarkProgress(progress);
            if (progress.Contains(progressSubstringToCancelToken))
            {
                cts.Cancel();
            }
        }
    }
}
