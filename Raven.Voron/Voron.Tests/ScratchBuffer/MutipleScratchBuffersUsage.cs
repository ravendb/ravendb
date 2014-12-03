// -----------------------------------------------------------------------
//  <copyright file="MutipleScratchBuffersHandling.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.ScratchBuffer
{
	public class MutipleScratchBuffersUsage : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxScratchBufferSize = 1024*1024*8; // 2048 pages
			options.MaxNumberOfPagesInJournalBeforeFlush = 128; 
		}

		[Fact]
		public void CanAddContinuallyGrowingValue()
		{
			// this test does not have any assertion - we just check here that we don't get ScratchBufferSizeLimitException

			var size = 0;

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(txw, "foo");

				txw.Commit();
			}

			// note that we cannot assume that we can't take the whole scratch because there is always a read transaction
			// also in the meanwhile the free space handling is doing its job so it needs some pages too
			// and we allocate not the exact size but the nearest power of two e.g. we write 257 pages but in scratch we request 512 ones

			while (size < 512 * AbstractPager.PageSize) 
			{
				using (var txr = Env.NewTransaction(TransactionFlags.Read))
				{
					using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
					{
						var tree = txw.ReadTree("foo");

						tree.Add("item", new byte[size]);

						txw.Commit();
					}
				}

				Thread.Sleep(10);
				Console.WriteLine ("succeeded for " + size);
				size += 1024;
			}
		}

		[Fact]
		public void CanAddContinuallyGrowingValue_ButNotCommitting()
		{
			// this test does not have any assertion - we just check here that we don't get ScratchBufferSizeLimitException

			var size = 0;

			using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(txw, "foo");

				txw.Commit();
			}

			// note that we cannot assume that we can't take the whole scratch because there is always a read transaction
			// also in the meanwhile the free space handling is doing its job so it needs some pages too
			// and we allocate not the exact size but the nearest power of two e.g. we write 257 pages but in scratch we request 512 ones

			while (size < 1024 * AbstractPager.PageSize)
			{
				using (var txr = Env.NewTransaction(TransactionFlags.Read))
				{
					using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
					{
						var tree = txw.ReadTree("foo");

						tree.Add("item", new byte[size]);

						// txw.Commit(); - intentionally not committing
					}
				}

				size += 1024;
			}
		} 
	}
}