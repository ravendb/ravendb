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

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.GetInMemory()))
			{
				var multiTrees = CreateTrees(env, 1, "multiTree");
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 120; i++)
					{
						foreach (var multiTreeName in multiTrees)
						{
						    var multiTree = tx.GetTree(multiTreeName);
							var id = Guid.NewGuid().ToString();

							addedIds.Add("test/0/user-" + i, id);

							multiTree.MultiAdd(tx, "test/0/user-" + i, id);
						}
					}

					foreach (var multiTreeName in multiTrees)
					{
                        var multiTree = tx.GetTree(multiTreeName);
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
                            var multiTree = tx.GetTree(multiTreeName);
					
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
                            var multiTree = tx.GetTree(multiTreeName);
					
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
	}
}