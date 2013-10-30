namespace Voron.Tests.Bugs
{
	using System.IO;

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
						tx.GetTree("tree" + i).Add(tx, "aaaa" + i, new MemoryStream());
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
						tx.GetTree("atree" + i).Add(tx, "aaaa" + i, new MemoryStream());
						tx.GetTree("btree" + i).MultiAdd(tx, "aaaa" + i, "aaaa" + i);
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
	}
}