namespace Voron.Tests.Bugs
{
	using System;
	using System.IO;
	using System.Text;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Deletes : StorageTest
	{
		[Fact]
		public void RebalancerIssue()
		{
			const int DocumentCount = 750;

			var rand = new Random();
			var testBuffer = new byte[757];
			rand.NextBytes(testBuffer);


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			var batch = new WriteBatch();
			for (var i = 0; i < DocumentCount; i++)
			{
				batch.Add("Foo" + i, new MemoryStream(testBuffer), "tree1");
			}

			Env.Writer.Write(batch);

			batch = new WriteBatch();
			for (var i = 0; i < DocumentCount; i++)
			{
				if (i >= 180)
					continue;

				batch.Delete("Foo" + i, "tree1");
			}

			Env.Writer.Write(batch);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
			    var t1 = tx.Environment.State.GetTree(tx,"tree1");
				t1.Delete(tx, "Foo180"); // rebalancer fails to move 1st node from one branch to another
			}
		}
	}
}
