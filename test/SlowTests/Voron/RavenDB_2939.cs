// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2939.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using FastTests.Voron;
using FastTests.Voron.Backups;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_2939 : StorageTest
    {
        private readonly IncrementalBackupTestUtils _incrementalBackupTestUtils = new IncrementalBackupTestUtils();

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldExplicitlyErrorThatTurningOnIncrementalBackupAfterInitializingTheStorageIsntAllowed()
        {
            RequireFileBasedPager();

            var random = new Random();
            var buffer = new byte[4000];
            random.NextBytes(buffer);

            for (int i = 0; i < 300; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, new MemoryStream(buffer));
                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();
            using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                op.SyncDataFile();
            }

            Env.Options.IncrementalBackupEnabled = true;

            var exception = Assert.Throws<InvalidOperationException>(() => BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0)));

            Assert.Equal("The first incremental backup creation failed because the first journal file " + StorageEnvironmentOptions.JournalName(0) + " was not found. Did you turn on the incremental backup feature after initializing the storage? In order to create backups incrementally the storage must be created with IncrementalBackupEnabled option set to 'true'.", exception.Message);
        }

        public override void Dispose()
        {
            base.Dispose();
            _incrementalBackupTestUtils.Dispose();
        }


    }
}
