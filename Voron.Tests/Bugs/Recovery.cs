namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;

	using Xunit;

	public class Recovery : StorageTest
	{
		[Fact]
		public void StorageRecoveryShouldWorkWhenThereAreNoTransactionsToRecoverFromLog()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkWhenThereSingleTransactionToRecoverFromLog()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "tree");

					for (var i = 0; i < 100; i++)
					{
						tree.Add(tx, "key" + i, new MemoryStream());
					}

					tx.Commit();
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "tree");

					tx.Commit();
				}


				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var tree = tx.GetTree("tree");

					for (var i = 0; i < 100; i++)
					{
						Assert.NotNull(tree.Read(tx, "key" + i));
					}
				}
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "tree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 10000; i++)
					{
						tx.GetTree("tree").Add(tx, "a" + i, new MemoryStream());
					}
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions2()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "atree");
					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 10000; i++)
					{
						tx.GetTree("atree").Add(tx, "a" + i, new MemoryStream());
						tx.GetTree("btree").MultiAdd(tx, "a" + i, "a" + i);
					}
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "atree");

					for (var i = 0; i < 1000; i++)
					{
						tree.Add(tx, "key" + i, new MemoryStream());
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "btree");

					for (var i = 0; i < 1; i++)
					{
						tree.Add(tx, "key" + i, new MemoryStream());
					}

					tx.Commit();
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "atree");
					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var aTree = tx.GetTree("atree");
					var bTree = tx.GetTree("btree");

					for (var i = 0; i < 1000; i++)
					{
						Assert.NotNull(aTree.Read(tx, "key" + i));
					}

					for (var i = 0; i < 1; i++)
					{
						Assert.NotNull(bTree.Read(tx, "key" + i));
					}
				}
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions2()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "atree");

					for (var i = 0; i < 1000; i++)
					{
						tree.Add(tx, "key" + i, new MemoryStream());
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = env.CreateTree(tx, "btree");

					for (var i = 0; i < 5; i++)
					{
						tree.Add(tx, "key" + i, new MemoryStream());
					}

					tx.Commit();
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "atree");
					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var aTree = tx.GetTree("atree");
					var bTree = tx.GetTree("btree");

					for (var i = 0; i < 1000; i++)
					{
						Assert.NotNull(aTree.Read(tx, "key" + i));
					}

					for (var i = 0; i < 5; i++)
					{
						Assert.NotNull(bTree.Read(tx, "key" + i));
					}
				}
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkForSplitTransactions()
		{
			var random = new Random(1234);
			var buffer = new byte[4096];
			random.NextBytes(buffer);
			var path = "test2.data";
			var count = 1000;

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			var options = StorageEnvironmentOptions.ForPath(path);
			options.MaxLogFileSize = 10 * options.DataPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "atree");
					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var aTree = tx.GetTree("atree");
					var bTree = tx.GetTree("btree");

					for (var i = 0; i < count; i++)
					{
						aTree.Add(tx, "a" + i, new MemoryStream(buffer));
						bTree.MultiAdd(tx, "a", "a" + i);
					}

					tx.Commit();
				}
			}

			var expectedString = Encoding.UTF8.GetString(buffer);

			options = StorageEnvironmentOptions.ForPath(path);
			options.MaxLogFileSize = 10 * options.DataPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "atree");
					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var aTree = tx.GetTree("atree");
					var bTree = tx.GetTree("btree");

					for (var i = 0; i < count; i++)
					{
						var read = aTree.Read(tx, "a" + i);
						Assert.NotNull(read);

						using (var reader = new StreamReader(read.Stream))
						{
							Assert.Equal(expectedString, reader.ReadToEnd());
						}
					}

					using (var iterator = bTree.MultiRead(tx, "a"))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var keys = new HashSet<string>();
						do
						{
							keys.Add(iterator.CurrentKey.ToString());
						}
						while (iterator.MoveNext());

						Assert.Equal(count, keys.Count);
					}
				}
			}
		}
	}
}