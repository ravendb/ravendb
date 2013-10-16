namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class PageSplitter : StorageTest
	{
		[Fact]
		public void PageSplitterShouldCalculateSeparatorKeyCorrectly()
		{
			var ids = ReadIds("data.txt");

			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[79];
				rand.NextBytes(testBuffer);

				var trees = CreateTrees(env, 1, "tree");

				var addedIds = new List<string>();
				foreach (var id in ids) // 244276974/13/250/2092845878 -> 8887 iteration
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						foreach (var tree in trees)
						{
							if (id == "244276974/13/250/2092845878")
							{
							}

							tree.Add(tx, id, new MemoryStream(testBuffer));
						}

						tx.Commit();

						addedIds.Add(id);
						if (addedIds.Count >= 8887)
						{
							//ValidateRecords(env, trees, addedIds);
						}
					}
				}

				ValidateRecords(env, trees, ids);
			}
		}

		[Fact]
		public void PageSplitterShouldCalculateSeparatorKeyCorrectly2()
		{
			var ids = ReadIds("data2.txt");

			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[69];
				rand.NextBytes(testBuffer);

				var trees = CreateTrees(env, 1, "tree");

				foreach (var id in ids)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						foreach (var tree in trees)
						{
							tree.Add(tx, id, new MemoryStream(testBuffer));
						}

						tx.Commit();
					}
				}

				ValidateRecords(env, trees, ids);
			}
		}

		private void ValidateRecords(StorageEnvironment env, IEnumerable<Tree> trees, IList<string> ids)
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
							Assert.True(ids.Contains(iterator.CurrentKey.ToString()));
							Assert.NotNull(snapshot.Read(tree.Name, iterator.CurrentKey));

							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(ids.Count, tree.State.EntriesCount);
						Assert.Equal(ids.Count, count);
						Assert.Equal(ids.Count, keys.Count);
					}
				}
			}
		}

		private static IList<string> ReadIds(string fileName)
		{
			using (var reader = new StreamReader("Bugs/Data/" + fileName))
			{
				string line;

				var results = new List<string>();

				while (!string.IsNullOrEmpty(line = reader.ReadLine()))
				{
					results.Add(line.Trim());
				}

				return results;
			}
		}
	}
}