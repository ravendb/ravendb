namespace Voron.Tests.Bugs
{
	using System;
	using System.IO;

	using Voron.Trees;

	using Xunit;

	public class Snapshots : StorageTest
	{
		[Fact]
		public void SnapshotIssue()
		{
			const int DocumentCount = 50000;

			var rand = new Random();
			var testBuffer = new byte[39];
			rand.NextBytes(testBuffer);

			Tree t1;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				t1 = Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < DocumentCount; i++)
				{
					t1.Add(tx, "docs/" + i, new MemoryStream(testBuffer));
				}

				tx.Commit();
			}

			using (var snapshot = Env.CreateSnapshot())
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (var i = 0; i < DocumentCount; i++)
					{
						t1.Delete(tx, "docs/" + i);
					}

					tx.Commit();
				}

				for (var i = 0; i < DocumentCount; i++)
				{
					var result = snapshot.Read(t1.Name, "docs/" + i);
					Assert.NotNull(result);

					using (var reader = new BinaryReader(result.Stream))
					{
						Assert.Equal(testBuffer, reader.ReadBytes((int)result.Stream.Length));
					}
				}
			}
		}
	}
}