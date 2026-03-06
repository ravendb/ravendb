using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron;
using FastTests.Voron.Backups;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_2939 : StorageTest
    {
        public RavenDB_2939(ITestOutputHelper output) : base(output)
        {
        }

        private readonly IncrementalBackupTestUtils _incrementalBackupTestUtils = new IncrementalBackupTestUtils();

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();
            _incrementalBackupTestUtils.Dispose();
        }


    }
}