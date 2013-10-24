namespace Voron.Tests.Storage
{
	using System.IO;

	using Voron.Exceptions;
	using Voron.Impl;

	using Xunit;

	public class Concurrency : StorageTest
	{
		[Fact]
		public void MissingEntriesShouldReturn0Version()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Equal(0, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void SimpleVersion()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(2, tx.State.Root.ReadVersion(tx, "key/1"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(2, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void VersionOverflow()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (uint i = 1; i <= ushort.MaxValue + 1; i++)
				{
					tx.State.Root.Add(tx, "key/1", StreamFor("123"));

					var expected = i;
					if (expected > ushort.MaxValue)
						expected = 1;

					Assert.Equal(expected, tx.State.Root.ReadVersion(tx, "key/1"));
				}

				tx.Commit();
			}
		}

		[Fact]
		public void NoCommit()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(2, tx.State.Root.ReadVersion(tx, "key/1"));
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(0, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void Delete()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));

				tx.State.Root.Delete(tx, "key/1");
				Assert.Equal(0, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void Missing()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", StreamFor("123"), 0);
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var e = Assert.Throws<ConcurrencyException>(() => tx.State.Root.Add(tx, "key/1", StreamFor("321"), 0));
				Assert.Equal("Cannot add 'key/1'. Version mismatch. Expected: 0. Actual: 1.", e.Message);
			}
		}

		[Fact]
		public void ConcurrencyExceptionShouldBeThrownWhenVersionMismatch()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", StreamFor("123"));
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var e = Assert.Throws<ConcurrencyException>(() => tx.State.Root.Add(tx, "key/1", StreamFor("321"), 2));
				Assert.Equal("Cannot add 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var e = Assert.Throws<ConcurrencyException>(() => tx.State.Root.Delete(tx, "key/1", 2));
				Assert.Equal("Cannot delete 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);
			}
		}

		[Fact]
		public void ConcurrencyExceptionShouldBeThrownWhenVersionMismatchMultiTree()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.MultiAdd(tx, "key/1", "123");
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var e = Assert.Throws<ConcurrencyException>(() => tx.State.Root.MultiAdd(tx, "key/1", "321", 2));
				Assert.Equal("Cannot add 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var e = Assert.Throws<ConcurrencyException>(() => tx.State.Root.MultiDelete(tx, "key/1", "123", 2));
				Assert.Equal("Cannot delete 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);
			}
		}

		[Fact]
		public void BatchSimpleVersion()
		{
			var batch1 = new WriteBatch();
            batch1.Add("key/1", StreamFor("123"), Constants.RootTreeName);

			Env.Writer.Write(batch1);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));
			}

			var batch2 = new WriteBatch();
			batch2.Add("key/1", StreamFor("123"), Constants.RootTreeName);

			Env.Writer.Write(batch2);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(2, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void BatchDelete()
		{
			var batch1 = new WriteBatch();
            batch1.Add("key/1", StreamFor("123"), Constants.RootTreeName);

			Env.Writer.Write(batch1);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));
			}

			var batch2 = new WriteBatch();
            batch2.Delete("key/1", Constants.RootTreeName);

			Env.Writer.Write(batch2);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(0, tx.State.Root.ReadVersion(tx, "key/1"));
			}
		}

		[Fact]
		public void BatchMissing()
		{
			var batch1 = new WriteBatch();
            batch1.Add("key/1", StreamFor("123"), Constants.RootTreeName, 0);

			Env.Writer.Write(batch1);

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(1, tx.State.Root.ReadVersion(tx, "key/1"));
			}

			var batch2 = new WriteBatch();
            batch2.Add("key/1", StreamFor("123"), Constants.RootTreeName, 0);

			var e = Assert.Throws<ConcurrencyException>(() => Env.Writer.Write(batch2));
			Assert.Equal("Cannot add 'key/1'. Version mismatch. Expected: 0. Actual: 1.", e.Message);
		}

		[Fact]
		public void BatchConcurrencyExceptionShouldBeThrownWhenVersionMismatch()
		{
			var batch1 = new WriteBatch();
            batch1.Add("key/1", StreamFor("123"), Constants.RootTreeName, 0);

			Env.Writer.Write(batch1);

			var batch2 = new WriteBatch();
            batch2.Add("key/1", StreamFor("123"), Constants.RootTreeName, 2);

			var e = Assert.Throws<ConcurrencyException>(() => Env.Writer.Write(batch2));
			Assert.Equal("Cannot add 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);

			var batch3 = new WriteBatch();
            batch3.Delete("key/1", Constants.RootTreeName, 2);

			e = Assert.Throws<ConcurrencyException>(() => Env.Writer.Write(batch3));
			Assert.Equal("Cannot delete 'key/1'. Version mismatch. Expected: 2. Actual: 1.", e.Message);
		}
	}
}