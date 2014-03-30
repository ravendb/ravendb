// -----------------------------------------------------------------------
//  <copyright file="UncommittedTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Journal
{
	public class UncommittedTransactions : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * AbstractPager.PageSize;
		}


		[Fact]
		public void UncommittedTransactionMustNotModifyPageTranslationTableOfLogFile()
		{
			long pageAllocatedInUncommittedTransaction;
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var page = tx1.AllocatePage(1);

				pageAllocatedInUncommittedTransaction = page.PageNumber;

				Assert.NotNull(tx1.GetReadOnlyPage(pageAllocatedInUncommittedTransaction));
				
				// tx.Commit(); do not commit
			}
			using (var tx2 = Env.NewTransaction(TransactionFlags.Read))
			{
				// tx was not committed so in the log should not apply
				var readPage = Env.Journal.ReadPage(tx2,pageAllocatedInUncommittedTransaction, scratchPagerState: null);

				Assert.Null(readPage);
			}
		}
	}
}