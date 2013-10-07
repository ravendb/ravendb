namespace Voron.Tests.Bugs
{
	using System;
	using System.IO;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Versioning : StorageTest
	{
		[Fact]
		public void SplittersAndRebalancersShouldNotChangeNodeVersion()
		{
			const int DocumentCount = 100000;

			var rand = new Random();
			var testBuffer = new byte[123];
			rand.NextBytes(testBuffer);

			Tree t1 = null;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			var batch = new WriteBatch();
			for (var i = 0; i < DocumentCount; i++)
			{
				batch.Add("Foo" + i, new MemoryStream(testBuffer), "tree1");
			}

			Env.Writer.Write(batch);

			batch = new WriteBatch();
			using (var snapshot = Env.CreateSnapshot())
			{
				for (var i = 0; i < DocumentCount; i++)
				{
					var result = snapshot.Read("tree1", "Foo" + 1, null);
					batch.Delete("Foo" + i, "tree1", result.Version);
				}
			}

			Env.Writer.Write(batch);
		}
	}
}