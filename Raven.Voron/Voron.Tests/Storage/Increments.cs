// -----------------------------------------------------------------------
//  <copyright file="Increments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Storage
{
	public class Increments : StorageTest
	{
		[Fact]
		public void SimpleIncrementShouldWork()
		{
			CreateTrees(Env, 1, "tree");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.ReadTree("tree0").Increment(tx, "key/1", 10);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.ReadTree("tree0").Increment(tx, "key/1", 5);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.ReadTree("tree0").Increment(tx, "key/1", -3);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var read = tx.ReadTree("tree0").Read(tx, "key/1");

				Assert.NotNull(read);
				Assert.Equal(3, read.Version);
				Assert.Equal(12, read.Reader.ReadInt64());
			}
		}
	}
}