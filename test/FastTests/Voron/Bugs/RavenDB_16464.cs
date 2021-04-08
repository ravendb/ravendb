using System;
using System.Collections.Generic;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_16464 : StorageTest
    {
        public RavenDB_16464(ITestOutputHelper output) : base(output)
        {
        }

        private const int _64KB = 64 * 1024;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 4;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = _64KB;
        }

        [Fact]
        public void ShouldFreeAllocationsInScratchBuffersSoTheyCanBeCleaned()
        {
            RequireFileBasedPager();

            var r = new Random(3);

            for (int j = 0; j < 2; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (int i = 0; i < 8; i++)
                    {
                        var overflowSize = r.Next(5, 10);

                        var bytes = new byte[overflowSize * 8192];

                        r.NextBytes(bytes);

                        tree.Add("items/" + i, bytes);
                    }

                    tx.Commit();
                }
            }

            Assert.Equal(3, Env.Journal.Files.Count); 
            Assert.Equal(0, Env.Journal.CurrentFile.Available4Kbs); // this is very important condition to run into the issue - see details in RavenDB-16464

            Env.FlushLogToDataFile(); // this will flush all journals, the issue was that it also marked all of them as unused so they were removed from _files list, but didn't free the allocations in scratch buffers to ensure we don't free pages that can be still read

            using (var readTx = Env.ReadTransaction())
            {
                var journalSnapshots = readTx.LowLevelTransaction.JournalSnapshots;

                Assert.Equal(0, journalSnapshots.Count); // _files collection was cleaned during the flush
            }

            Env.FlushLogToDataFile();

            Env.Cleanup(tryCleanupRecycledJournals: true);

            var scratchBufferPoolInfo = Env.ScratchBufferPool.InfoForDebug(Env.PossibleOldestReadTransaction(null));

            Assert.Equal(1, scratchBufferPoolInfo.ScratchFilesUsage.Count);
        }
    }
}
