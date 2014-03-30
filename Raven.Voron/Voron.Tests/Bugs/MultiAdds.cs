using System.Text;
using Xunit.Extensions;

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
		readonly Random _random = new Random(1234);

        private string RandomString(int size)
        {
            var builder = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                builder.Append(Convert.ToChar(Convert.ToInt32(Math.Floor(26 * _random.NextDouble() + 65))));
            }

            return builder.ToString();
        }

		[Theory]
        [InlineData(0500)]
        [InlineData(1000)]
        [InlineData(2000)]
        [InlineData(3000)]
        [InlineData(4000)]
        [InlineData(5000)]
		public void MultiAdds_And_MultiDeletes_After_Causing_PageSplit_DoNot_Fail(int size)
		{
			using (var Env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var inputData = new List<byte[]>();
				for (int i = 0; i < size; i++)
				{
                    inputData.Add(Encoding.UTF8.GetBytes(RandomString(1024)));
				}

				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					Env.CreateTree(tx, "foo");
					tx.Commit();
				}

				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.Environment.State.GetTree(tx,"foo");
					foreach (var buffer in inputData)
					{						
						Assert.DoesNotThrow(() => tree.MultiAdd(tx, "ChildTreeKey", new Slice(buffer)));
					}
					tx.Commit();
				}
				
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.Environment.State.GetTree(tx,"foo");
					for (int i = 0; i < inputData.Count; i++)
					{
						var buffer = inputData[i];
						Assert.DoesNotThrow(() => tree.MultiDelete(tx, "ChildTreeKey", new Slice(buffer)));
					}

					tx.Commit();
				}
			}
		}

		[Fact]
		public void SplitterIssue()
		{
			const int DocumentCount = 10;

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var rand = new Random();
				var testBuffer = new byte[168];
				rand.NextBytes(testBuffer);

				var multiTrees = CreateTrees(env, 1, "multitree");

				for (var i = 0; i < 50; i++)
				{
					AddMultiRecords(env, multiTrees, DocumentCount, true);

					ValidateMultiRecords(env, multiTrees, DocumentCount, i + 1);
				}
			}
		}

		[Fact]
		public void SplitterIssue2()
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
			storageEnvironmentOptions.ManualFlushing = true;
			using (var env = new StorageEnvironment(storageEnvironmentOptions))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "multi");
					tx.Commit();
				}

				var batch = new WriteBatch();

				batch.MultiAdd("0", "1", "multi");
				batch.MultiAdd("1", "1", "multi");
				batch.MultiAdd("2", "1", "multi");
				batch.MultiAdd("3", "1", "multi");
				batch.MultiAdd("4", "1", "multi");
				batch.MultiAdd("5", "1", "multi");

				env.Writer.Write(batch);

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var tree = tx.Environment.State.GetTree(tx,"multi");
					using (var iterator = tree.MultiRead(tx, "0"))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var count = 0;
						do
						{
							count++;
						} while (iterator.MoveNext());

						Assert.Equal(1, count);
					}
				}

				batch = new WriteBatch();

				batch.MultiAdd("0", "2", "multi");
				batch.MultiAdd("1", "2", "multi");
				batch.MultiAdd("2", "2", "multi");
				batch.MultiAdd("3", "2", "multi");
				batch.MultiAdd("4", "2", "multi");
				batch.MultiAdd("5", "2", "multi");

				env.Writer.Write(batch);

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var tree = tx.Environment.State.GetTree(tx,"multi");
					using (var iterator = tree.MultiRead(tx, "0"))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var count = 0;
						do
						{
							count++;
						} while (iterator.MoveNext());

						Assert.Equal(2, count);
					}
				}
			}
		}

		[Fact]
		public void CanAddMultiValuesUnderTheSameKeyToBatch()
		{
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var rand = new Random();
				var testBuffer = new byte[168];
				rand.NextBytes(testBuffer);

				CreateTrees(env, 1, "multitree");

				var batch = new WriteBatch();

				batch.MultiAdd("key", "value1", "multitree0");
				batch.MultiAdd("key", "value2", "multitree0");

				env.Writer.Write(batch);

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var tree = tx.Environment.State.GetTree(tx,"multitree0");
					using (var it = tree.MultiRead(tx, "key"))
					{
						Assert.True(it.Seek(Slice.BeforeAllKeys));

						Assert.Equal("value1", it.CurrentKey.ToString());
						Assert.True(it.MoveNext());

						Assert.Equal("value2", it.CurrentKey.ToString());
					}
				}
			}
		}

		private void ValidateRecords(StorageEnvironment env, IEnumerable<Tree> trees, int documentCount, int i)
		{
			using (var tx = env.NewTransaction(TransactionFlags.Read))
			{
				foreach (var tree in trees)
				{
					using (var iterator = tree.Iterate(tx))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var count = 0;
						do
						{
							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(i * documentCount, tree.State.EntriesCount);
						Assert.Equal(i * documentCount, count);
					}
				}
			}
		}

		private void ValidateMultiRecords(StorageEnvironment env, IEnumerable<string> trees, int documentCount, int i)
		{
			using (var tx = env.NewTransaction(TransactionFlags.Read))
			{
				for (var j = 0; j < 10; j++)
				{
					foreach (var treeName in trees)
					{
					    var tree = tx.Environment.State.GetTree(tx,treeName);
						using (var iterator = tree.MultiRead(tx, (j % 10).ToString()))
						{
							Assert.True(iterator.Seek(Slice.BeforeAllKeys));

							var count = 0;
							do
							{
								count++;
							}
							while (iterator.MoveNext());

							Assert.Equal((i * documentCount) / 10, count);
						}
					}
				}
			}
		}

		private void AddRecords(StorageEnvironment env, IList<Tree> trees, int documentCount, byte[] testBuffer, bool sequential)
		{
			var key = Guid.NewGuid().ToString();
			var batch = new WriteBatch();

			for (int i = 0; i < documentCount; i++)
			{
				foreach (var tree in trees)
				{
					var id = sequential ? string.Format("tree_{0}_record_{1}_key_{2}", tree.Name, i, key) : Guid.NewGuid().ToString();

					batch.Add(id, new MemoryStream(testBuffer), tree.Name);
				}
			}

			env.Writer.Write(batch);
		}

		private void AddMultiRecords(StorageEnvironment env, IList<string> trees, int documentCount, bool sequential)
		{
			var key = Guid.NewGuid().ToString();
			var batch = new WriteBatch();

			for (int i = 0; i < documentCount; i++)
			{
				foreach (var tree in trees)
				{
					var value = sequential ? string.Format("tree_{0}_record_{1}_key_{2}", tree, i, key) : Guid.NewGuid().ToString();

					batch.MultiAdd((i % 10).ToString(), value, tree);
				}
			}

			env.Writer.Write(batch);
		}

		private IList<string> CreateTrees(StorageEnvironment env, int number, string prefix)
		{
			var results = new List<string>();

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < number; i++)
				{
					results.Add(env.CreateTree(tx, prefix + i).Name);
				}

				tx.Commit();
			}

			return results;
		}
	}
}