using System;
using System.IO;
using FastTests.Voron;
using Sparrow;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12268 : StorageTest
    {
        [Fact]
        public void UsedScratchBuffersArentDeleted()
        {
            // prevent the work of the global flushing behavior
            Env.Options.MaxNumberOfPagesInJournalBeforeFlush = long.MaxValue;
            Env.Options.MaxScratchBufferSize = new Size(64, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");
                tx.Commit();
            }
            
            var random = new Random(1);
            var bytes = new byte[1024 * 8];

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");
                for (var i = 0; i < 128; i++)
                {
                    random.NextBytes(bytes);
                    tree.Add(i.ToString(), new MemoryStream(bytes));
                }

                tx.Commit();
            }

            using (var tx = Env.NewLowLevelTransaction(new TransactionPersistentContext(),
                TransactionFlags.ReadWrite, timeout: TimeSpan.FromMilliseconds(500)))
            {
                tx.ModifyPage(0);
                tx.Commit();
            }

            Env.BackgroundFlushWritesToDataFile();

            using (Env.ReadTransaction())
            {
                var infoForDebug = Env.ScratchBufferPool.InfoForDebug(0);
                var numberOfScratchBuffers = infoForDebug.NumberOfScratchFiles;

                Env.ScratchBufferPool.Cleanup();
                infoForDebug = Env.ScratchBufferPool.InfoForDebug(0);

                // the number of scratch buffers cannot be smaller since we are holding a read tx
                // but it can be larger because we are creating an empty write tx inside the cleanup
                Assert.True(numberOfScratchBuffers <= infoForDebug.NumberOfScratchFiles);
            }
        }
    }
}
