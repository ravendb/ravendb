// -----------------------------------------------------------------------
//  <copyright file="UncommittedTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Journal
{
	public class UncommittedTransactions : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * options.PageSize;
		}


		[Fact]
		public void UncommittedTransactionMustNotModifyPageTranslationTableOfLogFile()
		{
			long pageAllocatedInUncommittedTransaction;
			using (var tx1 = Env.WriteTransaction())
			{
			    var page = tx1.LowLevelTransaction.AllocatePage(1);

				pageAllocatedInUncommittedTransaction = page.PageNumber;

				Assert.NotNull(tx1.LowLevelTransaction.GetReadOnlyTreePage(pageAllocatedInUncommittedTransaction));
				
				// tx.Commit(); do not commit
			}
			using (var tx2 = Env.ReadTransaction())
			{
				// tx was not committed so in the log should not apply
				var readPage = Env.Journal.ReadPage(tx2.LowLevelTransaction,pageAllocatedInUncommittedTransaction, scratchPagerStates: null);

				Assert.Null(readPage);
			}
		}
	}
}