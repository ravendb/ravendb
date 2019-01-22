using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Voron;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_12725_4 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 1 * 1024 * 1024;
        }

        [Fact]
        public void Should_throw_on_missing_journal_during_recovery()
        {
            RequireFileBasedPager();

            var r = new Random(1);
            var bytes = new byte[512];

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char)j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char)j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            StopDatabase();

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;

            var firstJournal = new DirectoryInfo(journalPath).GetFiles("*.journal").OrderBy(x => x.Name).First();

            File.Delete(firstJournal.FullName);

            Assert.Throws<InvalidJournalException>(StartDatabase);
        }
    }
}
