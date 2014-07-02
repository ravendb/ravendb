// -----------------------------------------------------------------------
//  <copyright file="TreesWithPrefixedKeys.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Trees
{
	public class TreesWithPrefixedKeys : StorageTest
	{
		[Fact]
		public void BasicCheck()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "prefixed-tree", keysPrefixing: true);

				tree.Add("users/11", StreamFor("abc"));
				tree.Add("users/12", StreamFor("def"));
				tree.Add("users/20", StreamFor("ghi"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("prefixed-tree");

				Assert.Equal("abc", tree.Read("users/11").Reader.AsSlice());
				Assert.Equal("def", tree.Read("users/12").Reader.AsSlice());
				Assert.Equal("ghi", tree.Read("users/20").Reader.AsSlice());

				tx.Commit();
			}
		}

		[Fact]
		public void LotOfInsertsAndDeletes()
		{
			const int countOfTransactions = 10;
			const int iterations = 100;
			var emptyValue = new byte[0];

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "prefixed-tree", keysPrefixing: true);
				tx.Commit();
			}

			int counter = 0;

			for (int transactions = 0; transactions < countOfTransactions; transactions++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.ReadTree("prefixed-tree");

					for (var i = 0; i < iterations; i++)
					{
						tree.Add("prefixedItems/" + counter, emptyValue);
						tree.Add("prefixed/" + counter, emptyValue);

						counter++;
					}

					tx.Commit();
				}
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("prefixed-tree");

				for (int i = 0; i < iterations * countOfTransactions; i++)
				{
					var readResult = tree.Read("prefixedItems/" + i);
					Assert.NotNull(readResult);

					readResult = tree.Read("prefixed/" + i);
					Assert.NotNull(readResult);
				}
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("prefixed-tree");

				var treeIterator = tree.Iterate();

				Assert.True(treeIterator.Seek(Slice.BeforeAllKeys));

				var countOfItems = 0;

				do
				{
					Assert.True(treeIterator.CurrentKey.StartsWith((Slice)"prefixedItems/") || treeIterator.CurrentKey.StartsWith((Slice)"prefixed/"));
					countOfItems++;

				} while (treeIterator.MoveNext());

				Assert.Equal(countOfTransactions * iterations * 2, countOfItems);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = tx.ReadTree("prefixed-tree");

				counter = 0;

				for (var i = 0; i < iterations * countOfTransactions; i++)
				{
					tree.Delete("prefixedItems/" + counter);
					tree.Delete("prefixed/" + counter);

					counter++;
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("prefixed-tree");

				var treeIterator = tree.Iterate();

				Assert.False(treeIterator.Seek(Slice.BeforeAllKeys));
			}

		}
	}
}