// -----------------------------------------------------------------------
//  <copyright file="RecoveryWithManualFlush.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class RecoveryWithManualFlush : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.ManualFlushing = true;
		}

        [Fact]
        public void ShouldRecoverFromJournalsAfterFlushWhereLastPageOfFlushedTxHadTheSameNumberAsFirstPageOfNextTxNotFlushedJet()
        {
            using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx1.State.Root.Add(tx1, "item/1", new MemoryStream(new byte[4000]));
                tx1.State.Root.Add(tx1, "item/2", new MemoryStream(new byte[4000]));

                tx1.Commit();
            }

            using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                // update items/2 will change it 'in place' - will modify the same already existing page

                // this will also override the page translation table for the page where item/2 is placed

                tx2.State.Root.Add(tx2, "item/2", new MemoryStream(new byte[3999]));

                tx2.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
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

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var readResult = tx.State.Root.Read(tx, "item/1");

                Assert.NotNull(readResult);
                Assert.Equal(4000, readResult.Stream.Length);

                readResult = tx.State.Root.Read(tx, "item/2");

                Assert.NotNull(readResult);
                Assert.Equal(3999, readResult.Stream.Length);
            }
        }

        [Fact]
        public void ShouldRecoverTransactionEndPositionsTableAfterRestart()
        {
            using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx1.State.Root.Add(tx1, "item/1", new MemoryStream(new byte[4000]));
                tx1.State.Root.Add(tx1, "item/2", new MemoryStream(new byte[4000]));

                tx1.Commit();
            }

            using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx2.State.Root.Add(tx2, "item/2", new MemoryStream(new byte[3999]));

                tx2.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Env.FlushLogToDataFile();
            }

            StopDatabase();

            StartDatabase();

            Env.FlushLogToDataFile();

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var readResult = tx.State.Root.Read(tx, "item/1");

                Assert.NotNull(readResult);
                Assert.Equal(4000, readResult.Stream.Length);

                readResult = tx.State.Root.Read(tx, "item/2");

                Assert.NotNull(readResult);
                Assert.Equal(3999, readResult.Stream.Length);
            }
        }

		[Fact]
		public void StorageRecoveryAfterFlushingToDataFile()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "items/1", new MemoryStream(new byte[] { 1, 2, 3 }));
				tx.Commit();
			}

			Env.FlushLogToDataFile();

			RestartDatabase();

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readResult = tx.State.Root.Read(tx, "items/1");

				Assert.NotNull(readResult);
				Assert.Equal(3, readResult.Stream.Length);
			}
		} 
	}
}