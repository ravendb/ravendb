using System.IO;

namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using Xunit;

	public class TreeRebalancer : StorageTest
	{
		[Fact]
		public void TreeRabalancerShouldCopyNodeFlagsWhenMultiValuePageRefIsSet()
		{
			var addedIds = new Dictionary<string, string>();

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var multiTrees = CreateTrees(env, 1, "multiTree");
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 120; i++)
					{
						foreach (var multiTreeName in multiTrees)
						{
						    var multiTree = tx.Environment.State.GetTree(tx,multiTreeName);
							var id = Guid.NewGuid().ToString();

							addedIds.Add("test/0/user-" + i, id);

							multiTree.MultiAdd(tx, "test/0/user-" + i, id);
						}
					}

					foreach (var multiTreeName in multiTrees)
					{
                        var multiTree = tx.Environment.State.GetTree(tx,multiTreeName);
						multiTree.MultiAdd(tx, "test/0/user-50", Guid.NewGuid().ToString());
					}

					tx.Commit();
				}


				for (var i = 119; i > 99; i--)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
                        foreach (var multiTreeName in multiTrees)
                        {
                            var multiTree = tx.Environment.State.GetTree(tx,multiTreeName);
					
							multiTree.MultiDelete(tx, "test/0/user-" + i, addedIds["test/0/user-" + i]);
						}

						tx.Commit();
					}

					ValidateMulti(env, multiTrees);
				}

				for (var i = 0; i < 50; i++)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						if (i == 29)
						{
						}

                        foreach (var multiTreeName in multiTrees)
                        {
                            var multiTree = tx.Environment.State.GetTree(tx,multiTreeName);
					
							multiTree.MultiDelete(tx, "test/0/user-" + i, addedIds["test/0/user-" + i]);
						}

						tx.Commit();
					}

					ValidateMulti(env, multiTrees);
				}

				ValidateMulti(env, multiTrees);
			}
		}

		private void ValidateMulti(StorageEnvironment env, IEnumerable<string> trees)
		{
			using (var snapshot = env.CreateSnapshot())
			{
				foreach (var tree in trees)
				{
					using (var iterator = snapshot.MultiRead(tree, "test/0/user-50"))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var keys = new HashSet<string>();

						var count = 0;
						do
						{
							keys.Add(iterator.CurrentKey.ToString());
							Guid.Parse(iterator.CurrentKey.ToString());

							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(2, count);
						Assert.Equal(2, keys.Count);
					}
				}
			}
		}

		[Fact]
		public void ShouldNotThrowThatPageIsFullDuringTreeRebalancing()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "rebalancing-issue");

				var aKey = new string('a', 1024);
				var bKey = new string('b', 1024);
				var cKey = new string('c', 1024);
				var dKey = new string('d', 1024);
				var eKey = new string('e', 600);
				var fKey = new string('f', 920);

				tree.Add(tx, aKey, new MemoryStream(new byte[1000]));
				tree.Add(tx, bKey, new MemoryStream(new byte[1000]));
				tree.Add(tx, cKey, new MemoryStream(new byte[1000]));
				tree.Add(tx, dKey, new MemoryStream(new byte[1000]));
				tree.Add(tx, eKey, new MemoryStream(new byte[800]));
				tree.Add(tx, fKey, new MemoryStream(new byte[10]));

				RenderAndShow(tx, 1, "rebalancing-issue");

				// to expose the bug we need to delete the last item from the left most page
				// tree rebalance will try to fix the first reference (the implicit ref page node) in the parent page which is almost full 
				// and will fail because there is no space to put a new node

				tree.Delete(tx, aKey); // this line throws "The page is full and cannot add an entry, this is probably a bug"

				tx.Commit();

				using (var iterator = tree.Iterate(tx))
				{
					Assert.True(iterator.Seek(Slice.BeforeAllKeys));

					Assert.Equal(bKey, iterator.CurrentKey);
					Assert.True(iterator.MoveNext());

					Assert.Equal(cKey, iterator.CurrentKey);
					Assert.True(iterator.MoveNext());

					Assert.Equal(dKey, iterator.CurrentKey);
					Assert.True(iterator.MoveNext());

					Assert.Equal(eKey, iterator.CurrentKey);
					Assert.True(iterator.MoveNext());

					Assert.Equal(fKey, iterator.CurrentKey);
					Assert.False(iterator.MoveNext());
				}
			}
		}
	}
}