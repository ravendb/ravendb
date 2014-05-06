// -----------------------------------------------------------------------
//  <copyright file="MultiTreeSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests
{
	public class MultiTreeSize : StorageTest
	{
		[Fact]
		public void Single_AddMulti_WillUseOnePage()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.MultiAdd(tx, "ChildTreeKey", "test");
				tx.Commit();
			}

			Assert.Equal(AbstractPager.PageSize,
				Env.Stats().UsedDataFileSizeInBytes
			);
		}

		[Fact]
		public void TwoSmall_AddMulti_WillUseOnePage()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.MultiAdd(tx, "ChildTreeKey", "test1");
				tx.State.Root.MultiAdd(tx, "ChildTreeKey", "test2");
				tx.Commit();
			}

			Assert.Equal(AbstractPager.PageSize,
				Env.Stats().UsedDataFileSizeInBytes
			);
		}
	}
}