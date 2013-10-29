namespace Voron.Tests.Bugs
{
	using System.IO;

	using Xunit;

	public class Disposing : StorageTest
	{
		[Fact]
		public void DisposingAndRecreatingStorageShouldWork()
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
		public void DisposingAndRecreatingStorageShouldWork2()
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
			}
		}
	}
}