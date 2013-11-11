// -----------------------------------------------------------------------
//  <copyright file="ForceLogFlushes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Xunit;

namespace Voron.Tests.Journal
{
    public class BasicActions : StorageTest
    {
        // all tests here relay on the fact than one log file can contains max 10 pages
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * options.DataPager.PageSize;
	        options.ManualFlushing = true;
        }

        [Fact]
        public void CanUseMultipleLogFiles()
        {
            var bytes = new byte[1024];
            new Random().NextBytes(bytes);

            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.State.Root.Add(tx, "item/" + i, new MemoryStream(bytes));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);

            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.NotNull(tx.State.Root.Read(tx, "item/" + i));
                }
            }
        }

        [Fact]
        public void CanSplitTransactionIntoMultipleLogFiles()
        {
            var bytes = new byte[1024];
            new Random().NextBytes(bytes);

            // everything is done in one transaction
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 15; i++)
                {
                    tx.State.Root.Add(tx, "item/" + i, new MemoryStream(bytes));
                }

                tx.Commit();
            }

            // however we put that into 3 log files
            Assert.Equal(3, Env.Journal.Files.Count);

            // and still can read from both files
            for (var i = 0; i < 15; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.Read))
                {
                    Assert.NotNull(tx.State.Root.Read(tx, "item/" + i));
                }
            }
        }

        [Fact]
        public void ShouldNotReadUncommittedTransaction()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.State.Root.Add(tx, "items/1", StreamFor("values/1"));
                // tx.Commit(); uncommitted transaction
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Null(tx.State.Root.Read(tx, "items/1"));
            }
        }

        [Fact]
        public void ShouldMoveBackTheJournalsWritePagePositionAfterAbortedTransaction()
        {
			var writePosition = Env.Journal.CurrentFile.WritePagePosition;
	        
	        using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx1.State.Root.Add(tx1, "items/1", StreamFor("values/1"));

				Assert.True(Env.Journal.CurrentFile.WritePagePosition > writePosition);

                // tx1.Commit(); aborted transaction
            }

			Assert.Equal(0, Env.Journal.CurrentFile.Number); // still the same log
			Assert.Equal(writePosition, Env.Journal.CurrentFile.WritePagePosition); 
        }

        [Fact]
        public void CanFlushDataFromLogToDataFile()
        {
            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.State.Root.Add(tx, "items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }

            Assert.True(Env.Journal.Files.Count > 1);
            var usedLogFiles = Env.Journal.Files.Count;

            Env.FlushLogToDataFile();

            Assert.True(Env.Journal.Files.Count <= 1 && Env.Journal.Files.Count < usedLogFiles);

            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.Read))
                {
                    var readKey = ReadKey(tx, "items/" + i);
                    Assert.Equal("values/" + i, readKey.Item2);
                }
            }
        }
    }
}