using Xunit;

namespace Voron.Tests.Trees
{
	using System;

	public class MultipleTrees : StorageTest
	{
		[Fact]
		public void CanCreateNewTree()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "test");

				Env.CreateTree(tx, "test").Add(tx, "test", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var stream = tx.Environment.State.GetTree(tx,"test").Read(tx, "test");
				Assert.NotNull(stream);

				tx.Commit();
			}
		}

		[Fact]
		public void CanUpdateValuesInSubTree()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "test");

				Env.CreateTree(tx, "test").Add(tx, "test", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{

				tx.Environment.State.GetTree(tx,"test").Add(tx, "test2", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var stream = tx.Environment.State.GetTree(tx,"test").Read(tx, "test2");
				Assert.NotNull(stream);

				tx.Commit();
			}
		}

		[Fact]
		public void CreatingTreeWithoutCommitingTransactionShouldYieldNoResults()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "test");
			}

			var e = Assert.Throws<InvalidOperationException>(() =>
			    {
			        using (var tx = Env.NewTransaction(TransactionFlags.Read))
			        {
			            tx.Environment.State.GetTree(tx,"test");
			        }
			    });
			Assert.Equal("No such tree: test", e.Message);
		}
	}
}
