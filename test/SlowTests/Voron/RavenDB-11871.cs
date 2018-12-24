using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests.Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_11871 : StorageTest
    {
        [Fact]
        public void WillNotRetainJournalsAfterSync()
        {
            RequireFileBasedPager();

            Options.ManualFlushing = true;
            Options.MaxLogFileSize = 1024 * 1024;

            for (int X = 0; X < 3; X++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("t");

                    for (int i = 0; i < 50_000; i++)
                    {
                        tree.Add(i.ToString() + "-" + X, Guid.NewGuid().ToByteArray());
                    }

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            var journalsDir = Path.Combine(DataDir, "Journals");

            Assert.NotEmpty(Directory.GetFiles(journalsDir, "*.journal"));

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");

                for (int i = 0; i < 50_000; i++)
                {
                    tree.Add(i.ToString() + "-5", Guid.NewGuid().ToByteArray());
                }

                tx.Commit();
            }

            using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                op.SyncDataFile();
            }
            Env.FlushLogToDataFile();

            using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                op.SyncDataFile();
            }

            Assert.Empty(Directory.GetFiles(journalsDir, "*.journal"));
        }
    }
}
