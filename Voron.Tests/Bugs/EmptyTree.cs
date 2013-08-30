using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class EmptyTree : StorageTest
	{
		 [Fact]
		 public void ShouldBeEmpty()
		 {
			 using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			 {
				 Env.CreateTree(tx, "events");

				 tx.Commit();
			 }

			 using (var tx = Env.NewTransaction(TransactionFlags.Read))
			 {
				 var treeIterator = Env.GetTree(tx, "events").Iterate(tx);

				 Assert.False(treeIterator.Seek(Slice.AfterAllKeys));

				 tx.Commit();
			 }
		 }

		 [Fact]
		 public void SurviveRestart()
		 {
			using (var pager = new PureMemoryPager())
			{
				using (var env = new StorageEnvironment(pager, ownsPager: false))
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.CreateTree(tx, "events");

						tx.Commit();
					}

					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.GetTree(tx,"events").Add(tx, "test", new MemoryStream(0));

						tx.Commit();
					}
				}

				using (var env = new StorageEnvironment(pager, ownsPager: false))
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.CreateTree(tx, "events");

						tx.Commit();
					}

					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						Assert.NotNull(env.GetTree(tx,"events").Read(tx, "test"));

						tx.Commit();
					}
				}
			}

			
		 }
	}
}