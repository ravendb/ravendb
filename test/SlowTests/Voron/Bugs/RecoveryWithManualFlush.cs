// -----------------------------------------------------------------------
//  <copyright file="RecoveryWithManualFlush.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RecoveryWithManualFlush : FastTests.Voron.StorageTest
    {
        public RecoveryWithManualFlush(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldRecoverFromJournalsAfterFlushWhereLastPageOfFlushedTxHadTheSameNumberAsFirstPageOfNextTxNotFlushedJet()
        {
            using (var tx1 = Env.WriteTransaction())
            {
                var tree = tx1.CreateTree("foo");
                tree.Add("item/1", new MemoryStream(new byte[4000]));
                tree.Add("item/2", new MemoryStream(new byte[4000]));

                tx1.Commit();
            }

            using (var tx2 = Env.WriteTransaction())
            {
                // update items/2 will change it 'in place' - will modify the same already existing page

                // this will also override the page translation table for the page where item/2 is placed

                var tree = tx2.CreateTree("foo");
                tree.Add("item/2", new MemoryStream(new byte[3999]));

                tx2.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                // here we have to flush inside the read transaction to ensure that
                // the oldest active transaction id is the same as id of tx2

                // the issue is that now we use journal's page translation table (PTT) to determine which page is
                // the last synced journal page but we overwrote it in the PTT by next transaction (tx2) that updated this page
                // so in the PTT we have only the most updated version of the page but we lost the information about
                // the last page of the last flushed transaction from journal

                Env.FlushLogToDataFile();
            }

            StopDatabase();

            StartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read("item/1");

                Assert.NotNull(readResult);
                Assert.Equal(4000, readResult.Reader.Length);

                readResult = tree.Read("item/2");

                Assert.NotNull(readResult);
                Assert.Equal(3999, readResult.Reader.Length);
            }
        }

        [Fact]
        public void ShouldRecoverTransactionEndPositionsTableAfterRestart()
        {
            using (var tx1 = Env.WriteTransaction())
            {
                var tree = tx1.CreateTree("foo");
                tree.Add("item/1", new MemoryStream(new byte[4000]));
                tree.Add("item/2", new MemoryStream(new byte[4000]));

                tx1.Commit();
            }

            using (var tx2 = Env.WriteTransaction())
            {
                var tree = tx2.CreateTree("foo");
                tree.Add("item/2", new MemoryStream(new byte[3999]));

                tx2.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Env.FlushLogToDataFile();
            }

            StopDatabase();

            StartDatabase();

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read("item/1");

                Assert.NotNull(readResult);
                Assert.Equal(4000, readResult.Reader.Length);

                readResult = tree.Read("item/2");

                Assert.NotNull(readResult);
                Assert.Equal(3999, readResult.Reader.Length);
            }
        }

        [Fact]
        public void StorageRecoveryAfterFlushingToDataFile()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("items/1", new MemoryStream(new byte[] { 1, 2, 3 }));
                tx.Commit();
            }

            Env.FlushLogToDataFile();

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read("items/1");

                Assert.NotNull(readResult);
                Assert.Equal(3, readResult.Reader.Length);
            }
        }
    }
}
