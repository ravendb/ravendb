namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;

	using Voron.Debugging;
	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class MultiAdds
	{
		[Fact]
		public void SplitterIssue()
		{
			const int DocumentCount = 10;

			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[168];
				rand.NextBytes(testBuffer);

				var multiTrees = CreateTrees(env, 1, "multitree");

				for (var i = 0; i < 50; i++)
				{
					AddMultiRecords(env, multiTrees, DocumentCount, true, i);

					ValidateMultiRecords(env, multiTrees, DocumentCount, i + 1);
				}
			}
		}

		[Fact]
		public void T2()
		{
			const int DocumentCount = 500;

			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[22];
				rand.NextBytes(testBuffer);

				var trees = CreateTrees(env, 20, "tree");
				var multiTrees = CreateTrees(env, 20, "multitree");

				for (var i = 0; i < 10; i++)
				{
					AddRecords(env, trees, DocumentCount, testBuffer, true, i);
					AddMultiRecords(env, multiTrees, DocumentCount, true, i);

					AddRecords(env, trees, DocumentCount, testBuffer, true, i);
					AddMultiRecords(env, multiTrees, DocumentCount, true, i);

					ValidateRecords(env, trees, DocumentCount, i + 1);
				}
			}
		}

		private void DeleteMultiRecords(StorageEnvironment env, IList<Tree> trees, int documentCount, bool sequential, int key)
		{
			var batch = new WriteBatch();

			for (int i = 0; i < documentCount; i++)
			{
				foreach (var tree in trees)
				{
					var value = sequential ? string.Format("{0}_tree_{0}_record_{1}_key_{2}", tree.Name, i, key) : Guid.NewGuid().ToString();

					batch.MultiDelete((i % 10).ToString(), value, tree.Name);
				}
			}

			env.Writer.Write(batch);
		}

		private void ValidateRecords(StorageEnvironment env, IEnumerable<Tree> trees, int documentCount, int i)
		{
			using (var snapshot = env.CreateSnapshot())
			{
				foreach (var tree in trees)
				{
					using (var iterator = snapshot.Iterate(tree.Name))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var keys = new HashSet<string>();

						var count = 0;
						do
						{
							keys.Add(iterator.CurrentKey.ToString());
							Assert.NotNull(snapshot.Read(tree.Name, iterator.CurrentKey));

							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(i * documentCount, tree.State.EntriesCount);
						Assert.Equal(i * documentCount, count);
						Assert.Equal(i * documentCount, keys.Count);
					}
				}
			}
		}

		private void ValidateMultiRecords(StorageEnvironment env, IEnumerable<Tree> trees, int documentCount, int i)
		{
			using (var tx = env.NewTransaction(TransactionFlags.Read))
			{
				for (var j = 0; j < 10; j++)
				{
					foreach (var tree in trees)
					{
						using (var iterator = tree.MultiRead(tx, (j % 10).ToString()))
						{
							var valid = iterator.Seek(Slice.BeforeAllKeys);
							if (documentCount > 0)
								Assert.True(valid);
							else
							{
								Assert.False(valid);
								continue;
							}

							var keys = new HashSet<string>();

							var count = 0;
							do
							{
								keys.Add(iterator.CurrentKey.ToString());

								count++;
							}
							while (iterator.MoveNext());

							Assert.Equal((i * documentCount) / 10, count);
							Assert.Equal((i * documentCount) / 10, keys.Count);
						}
					}
				}
			}
		}

		private void AddRecords(StorageEnvironment env, IList<Tree> trees, int documentCount, byte[] testBuffer, bool sequential, int key)
		{
			var batch = new WriteBatch();

			for (int i = 0; i < documentCount; i++)
			{
				var id = sequential ? string.Format("{0}_record_{0}_key_{1}", i, key) : Guid.NewGuid().ToString();

				foreach (var tree in trees)
				{
					batch.Add(id, new MemoryStream(testBuffer), tree.Name);
				}
			}

			env.Writer.Write(batch);
		}

		private void AddMultiRecords(StorageEnvironment env, IList<Tree> trees, int documentCount, bool sequential, int key)
		{
			var batch = new WriteBatch();

			for (int i = 0; i < documentCount; i++)
			{
				foreach (var tree in trees)
				{
					var value = sequential ? string.Format("{0}_tree_{0}_record_{1}_key_{2}", tree.Name, i, key) : Guid.NewGuid().ToString();

					batch.MultiAdd((i % 10).ToString(), value, tree.Name);
				}
			}

			env.Writer.Write(batch);
		}

		private IList<Tree> CreateTrees(StorageEnvironment env, int number, string prefix)
		{
			var results = new List<Tree>();

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < number; i++)
				{
					results.Add(env.CreateTree(tx, prefix + i));
				}

				tx.Commit();
			}

			return results;
		}
	}
}