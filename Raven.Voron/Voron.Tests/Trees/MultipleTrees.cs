using Voron.Util;
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

				Env.CreateTree(tx, "test").Add("test", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var stream = tx.Environment.CreateTree(tx,"test").Read("test");
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

				Env.CreateTree(tx, "test").Add("test", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{

				tx.Environment.CreateTree(tx,"test").Add("test2", StreamFor("abc"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var stream = tx.Environment.CreateTree(tx,"test").Read("test2");
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
			            tx.Environment.CreateTree(tx,"test");
			        }
			    });
			Assert.Contains("No such tree: test", e.Message);
		}
	}
}
