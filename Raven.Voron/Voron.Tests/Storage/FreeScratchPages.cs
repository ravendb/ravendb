// -----------------------------------------------------------------------
//  <copyright file="FreeScratchPages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Voron.Impl.Scratch;
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

            HashSet<PageFromScratchBuffer> scratchPagesOfUncommittedTransaction = new HashSet<PageFromScratchBuffer>();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 10; i++)
				{
					tx.State.Root.Add("items/" + i, new MemoryStream(buffer));
				}

                // For optimization purposes the used scratch pages now do not include the transaction header, therefore we need to include it.
                scratchPagesOfUncommittedTransaction = new HashSet<PageFromScratchBuffer>(tx.GetTransactionPages());
                scratchPagesOfUncommittedTransaction.Add(tx.GetTransactionHeaderPage());

				// tx.Commit() - intentionally not committing
			}

			HashSet<PageFromScratchBuffer> scratchPagesOfCommittedTransaction = new HashSet<PageFromScratchBuffer>();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				// let's do exactly the same, it should reuse the same scratch pages
				for (int i = 0; i < 10; i++)
				{
					tx.State.Root.Add("items/" + i, new MemoryStream(buffer));
				}

                // For optimization purposes the used scratch pages now do not include the transaction header, therefore we need to include it.
                scratchPagesOfCommittedTransaction = new HashSet<PageFromScratchBuffer>(tx.GetTransactionPages());
                scratchPagesOfCommittedTransaction.Add(tx.GetTransactionHeaderPage());

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