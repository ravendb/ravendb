// -----------------------------------------------------------------------
//  <copyright file="TreeStateTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Debugging;
using Voron.Trees;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Trees
{
	public class TreeStateTests : StorageTest
	{
		[Theory]
		[InlineData(5, 2)]
		[InlineData(35, 13)]
		[InlineData(256, 32)]
		public void TotalPageCountConsistsOfLeafBrancheAndOverflowPages(int regularItemsCount, int overflowsCount)
		{
			var r = new Random();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < regularItemsCount; i++)
				{
					tx.State.Root.Add("test" + new string('-', r.Next(128)) + i, new byte[r.Next(512)]);
				}

				for (int i = 0; i < overflowsCount; i++)
				{
					tx.State.Root.Add("overflow" + new string('-', r.Next(128)) + i, new byte[r.Next(8192)]);
				}

				tx.Commit();

				var treeState = tx.State.Root.State;

				Assert.True(treeState.PageCount > 0);
				Assert.Equal(treeState.PageCount, treeState.BranchPages + treeState.LeafPages + treeState.OverflowPages);
			}
		}

		[Fact]
		public void HasReducedNumberOfPagesAfterRemovingHalfOfEntries()
		{
			const int numberOfRegularItems = 256;
			const int numberOfOverflowItems = 3;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < numberOfRegularItems; i++)
				{
					tx.State.Root.Add("test" + new string('-', 128) + i, new byte[256]);
				}

				for (int i = 0; i < numberOfOverflowItems; i++)
				{
					tx.State.Root.Add("overflow" + new string('-', 128) + i, new byte[8192]);
				}

				tx.Commit();

				Assert.Equal(50, tx.State.Root.State.PageCount);
				Assert.Equal(38, tx.State.Root.State.LeafPages);
				Assert.Equal(3, tx.State.Root.State.BranchPages);
				Assert.Equal(9, tx.State.Root.State.OverflowPages);
				Assert.Equal(3, tx.State.Root.State.Depth);				
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < numberOfRegularItems / 2; i++)
				{
					tx.State.Root.Delete("test" + new string('-', 128) + i);
				}

				tx.State.Root.Delete("overflow" + new string('-', 128) + 0);

				tx.Commit();

				DebugStuff.RenderAndShow(tx.State.Root);

				Assert.Equal(31, tx.State.Root.AllPages().Count);
				Assert.Equal(31, tx.State.Root.State.PageCount);
				Assert.Equal(22, tx.State.Root.State.LeafPages);
				Assert.Equal(3, tx.State.Root.State.BranchPages);
				Assert.Equal(6, tx.State.Root.State.OverflowPages);
				Assert.Equal(3, tx.State.Root.State.Depth);

			}
		} 
	}
}