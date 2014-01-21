// -----------------------------------------------------------------------
//  <copyright file="FreeScratchPages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
	public class FreeScratchPages : StorageTest
	{
		[Fact]
		public void UncommittedTransactionShouldFreeScratchPagesThatWillBeReusedByNextTransaction()
		{
			var random = new Random();
			var buffer = new byte[1024];
			random.NextBytes(buffer);

			List<PageFromScratchBuffer> scratchPagesOfUncommittedTransaction;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 10; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				scratchPagesOfUncommittedTransaction = tx.GetTransactionPages();

				// tx.Commit() - intentionally not committing
			}

			List<PageFromScratchBuffer> scratchPagesOfCommittedTransaction;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				// let's do exactly the same, it should reuse the same scratch pages
				for (int i = 0; i < 10; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				scratchPagesOfCommittedTransaction = tx.GetTransactionPages();

				tx.Commit();
			}

			Assert.Equal(scratchPagesOfUncommittedTransaction.Count, scratchPagesOfCommittedTransaction.Count);

			foreach (var uncommittedPage in scratchPagesOfUncommittedTransaction)
			{
				Assert.Contains(uncommittedPage, scratchPagesOfCommittedTransaction);
			}
		}
	}
}