// -----------------------------------------------------------------------
//  <copyright file="MultiReads.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Bugs
{
	public class MultiReads : StorageTest
	{
		[Fact]
		public void MultiReadShouldKeepItemOrder()
		{
			foreach (var treeName in CreateTrees(Env, 1, "tree"))
			{
				using (var tx = Env.WriteTransaction())
				{
					tx.ReadTree(treeName).MultiAdd("queue1", "queue1/07000000-0000-0000-0000-000000000001");
					tx.ReadTree(treeName).MultiAdd("queue1", "queue1/07000000-0000-0000-0000-000000000002");

					tx.Commit();
				}

				using (var snapshot = Env.ReadTransaction())
				using (var iterator = snapshot.CreateTree(treeName).MultiRead("queue1"))
				{
					Assert.True(iterator.Seek(Slice.BeforeAllKeys));

					Assert.Equal("queue1/07000000-0000-0000-0000-000000000001", iterator.CurrentKey.ToString());
					Assert.True(iterator.MoveNext());
					Assert.Equal("queue1/07000000-0000-0000-0000-000000000002", iterator.CurrentKey.ToString());
					Assert.False(iterator.MoveNext());
				}
			}
		}
	}
}