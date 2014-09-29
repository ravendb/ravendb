// -----------------------------------------------------------------------
//  <copyright file="MutipleScratchBuffersHandling.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.ScratchBuffer
{
	public class MutipleScratchBuffersUsage : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxScratchBufferSize = 1024*1024*8;
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

			while (size < 1024 * AbstractPager.PageSize) // note that we cannot assume that we can take the whole scratch because there is always a read transaction, also note that we always allocate a size which is the nearest power of 2
			{
				using (var txr = Env.NewTransaction(TransactionFlags.Read))
				{
					using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
					{
						var tree = txw.ReadTree("foo");

						tree.Add("item", new byte[size]);
					}
				}

				size += 128;
			}
		} 
	}
}