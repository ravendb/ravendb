// -----------------------------------------------------------------------
//  <copyright file="Increments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron.Impl;

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
				Assert.Equal(10, tx.ReadTree("tree0").Increment(tx, "key/1", 10));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Equal(15, tx.ReadTree("tree0").Increment(tx, "key/1", 5));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Equal(12, tx.ReadTree("tree0").Increment(tx, "key/1", -3));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var read = tx.ReadTree("tree0").Read(tx, "key/1");

				Assert.NotNull(read);
				Assert.Equal(3, read.Version);
				Assert.Equal(12, read.Reader.ReadLittleEndianInt64());
			}
		}

		[Fact]
		public void SimpleIncrementShouldWorkUsingWriteBatch()
		{
			CreateTrees(Env, 1, "tree");

			var writeBatch = new WriteBatch();
			writeBatch.Increment("key/1", 10, "tree0");

			Env.Writer.Write(writeBatch);

			writeBatch = new WriteBatch();
			writeBatch.Increment("key/1", 5, "tree0");

			Env.Writer.Write(writeBatch);

			writeBatch = new WriteBatch();
			writeBatch.Increment("key/1", -3, "tree0");

			Env.Writer.Write(writeBatch);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var read = tx.ReadTree("tree0").Read(tx, "key/1");

				Assert.NotNull(read);
				Assert.Equal(3, read.Version);
				Assert.Equal(12, read.Reader.ReadLittleEndianInt64());
			}
		}
	}
}