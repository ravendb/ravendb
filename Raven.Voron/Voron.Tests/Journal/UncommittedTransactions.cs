// -----------------------------------------------------------------------
//  <copyright file="UncommittedTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Log
{
	public class UncommittedTransactions : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * options.DataPager.PageSize;
		}

		[Fact]
		public void ShouldReusePagesOfUncommittedTransactionEvenIfItFilledTheLogCompletely()
		{
			using (var tx0 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[4 * Env.PageSize]; 
                tx0.State.Root.Add(tx0, "items/0", new MemoryStream(bytes));
				//tx0.Commit(); intentionally
			}

			Assert.Equal(0, Env.Journal.CurrentFile.AvailablePages);

			var writePositionAfterUncommittedTransaction = Env.Journal.CurrentFile.WritePagePosition;

			// should reuse pages allocated by uncommitted tx0
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[2 * Env.PageSize]; // here we allocate less pages
                tx1.State.Root.Add(tx1, "items/1", new MemoryStream(bytes));
				tx1.Commit();
			}

			Assert.Equal(0, Env.Journal.CurrentFile.Number);
			Assert.True(Env.Journal.CurrentFile.WritePagePosition < writePositionAfterUncommittedTransaction);
		}

		[Fact]
		public void UncommittedTransactionMustNotModifyPageTranslationTableOfLogFile()
		{
			long pageAllocatedInUncommittedTransaction;
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var page = tx1.AllocatePage(1);

				pageAllocatedInUncommittedTransaction = page.PageNumber;

				Assert.NotNull(Env.Journal.ReadPage(tx1, pageAllocatedInUncommittedTransaction));
				
				// tx.Commit(); do not commit
			}
			using (var tx2 = Env.NewTransaction(TransactionFlags.Read))
			{
				// tx was not committed so in the log should not apply
				var readPage = Env.Journal.ReadPage(tx2, pageAllocatedInUncommittedTransaction);

				Assert.Null(readPage);
			}
		}

		[Fact]
		public void LogShouldCopeWithUncommittedSplitTransaction()
		{
			var bytes = new byte[1024];
			new Random().NextBytes(bytes);

			// everything is done in one transaction but it takes 3 log files - transaction split
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.True(Env.Journal.Files.Count == 1);

				for (int i = 0; i < 15; i++)
				{
                    tx.State.Root.Add(tx, "item/" + i, new MemoryStream(bytes));
				}

				Assert.Equal(3, Env.Journal.Files.Count); // verify that it really takes 3 pages

				// tx.Commit(); do not commit
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				// should go to first log because the last split transaction was uncommitted so we can reuse the pages allocated by it
				Assert.Equal(0, Env.Journal.CurrentFile.Number);

                tx.State.Root.Add(tx, "item/a", new MemoryStream(bytes));
				
				tx.Commit();
			}
		}
	}
}