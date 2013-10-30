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
					for (var i = 0; i < 100; i++)
					{
						env.CreateTree(tx, "tree" + i);
					}

					tx.Commit();
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (var i = 0; i < 100; i++)
					{
						Assert.NotNull(tx.GetTree("tree" + i));
					}
				}
			}
		}

		[Fact]
		public void StorageRecoveryShouldWorkForWhenThereAreCommitedAndUncommitedTransactions()
		{
			var path = "test2.data";

			if (Directory.Exists(path))
				Directory.Delete(path, true);

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 10000; i++)
					{
						env.CreateTree(tx, "tree" + i);
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 10000; i++)
					{
						tx.GetTree("tree" + i).Add(tx, "a" + i, new MemoryStream());
					}
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (var i = 0; i < 10000; i++)
					{
						Assert.NotNull(tx.GetTree("tree" + i));
					}
				}
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
					for (var i = 0; i < 10000; i++)
					{
						env.CreateTree(tx, "atree" + i);
						env.CreateTree(tx, "btree" + i);
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 10000; i++)
					{
						tx.GetTree("atree" + i).Add(tx, "a" + i, new MemoryStream());
						tx.GetTree("btree" + i).MultiAdd(tx, "a" + i, "a" + i);
					}
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (var i = 0; i < 10000; i++)
					{
						Assert.NotNull(tx.GetTree("atree" + i));
						Assert.NotNull(tx.GetTree("btree" + i));
					}
				}
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
					for (var i = 0; i < 100; i++)
					{
						env.CreateTree(tx, "atree" + i);
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < 1; i++)
					{
						env.CreateTree(tx, "btree" + i);
					}

					tx.Commit();
				}
			}

			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (var i = 0; i < 100; i++)
					{
						Assert.NotNull(tx.GetTree("atree" + i));
					}

					for (var i = 0; i < 1; i++)
					{
						Assert.NotNull(tx.GetTree("btree" + i));
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
			options.LogFileSize = 10 * options.DataPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < count; i++)
					{
						env.CreateTree(tx, "atree" + i);
					}

					env.CreateTree(tx, "btree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var bTree = tx.GetTree("btree");

					for (var i = 0; i < count; i++)
					{
						tx.GetTree("atree" + i).Add(tx, "a" + i, new MemoryStream(buffer));
						bTree.MultiAdd(tx, "a", "a" + i);
					}

					tx.Commit();
				}
			}

			var expectedString = Encoding.UTF8.GetString(buffer);

			options = StorageEnvironmentOptions.ForPath(path);
			options.LogFileSize = 10 * options.DataPager.PageSize;

			using (var env = new StorageEnvironment(options))
			{
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var bTree = tx.GetTree("btree");
					Assert.NotNull(bTree);

					for (var i = 0; i < count; i++)
					{
						var aTree = tx.GetTree("atree" + i);
						Assert.NotNull(aTree);

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