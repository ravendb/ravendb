using System;
using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
	public class Updates : StorageTest
	{
		[Fact]
		public void CanUpdateVeryLargeValueAndThenDeleteIt()
		{
			var random = new Random();
			var buffer = new byte[8192];
			random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "a", new MemoryStream(buffer));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(4, tx.State.Root.State.PageCount);
				Assert.Equal(3, tx.State.Root.State.OverflowPages);
			}

			buffer = new byte[8192 * 2];
			random.NextBytes(buffer);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "a", new MemoryStream(buffer));

				tx.Commit();
			}


			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Equal(6, tx.State.Root.State.PageCount);
				Assert.Equal(5, tx.State.Root.State.OverflowPages);				
			}
		}


		[Fact]
		public void CanAddAndUpdate()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test", StreamFor("1"));
				tx.State.Root.Add(tx, "test", StreamFor("2"));

				var readKey = ReadKey(tx, "test");
				Assert.Equal("test", readKey.Item1);
				Assert.Equal("2", readKey.Item2);
			}
		}

		[Fact]
		public void CanAddAndUpdate2()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test/1", StreamFor("1"));
				tx.State.Root.Add(tx, "test/2", StreamFor("2"));
				tx.State.Root.Add(tx, "test/1", StreamFor("3"));

				var readKey = ReadKey(tx, "test/1");
				Assert.Equal("test/1", readKey.Item1);
				Assert.Equal("3", readKey.Item2);

				readKey = ReadKey(tx, "test/2");
				Assert.Equal("test/2", readKey.Item1);
				Assert.Equal("2", readKey.Item2);

			}
		}

		[Fact]
		public void CanAddAndUpdate1()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test/1", StreamFor("1"));
				tx.State.Root.Add(tx, "test/2", StreamFor("2"));
				tx.State.Root.Add(tx, "test/2", StreamFor("3"));

				var readKey = ReadKey(tx, "test/1");
				Assert.Equal("test/1", readKey.Item1);
				Assert.Equal("1", readKey.Item2);

				readKey = ReadKey(tx, "test/2");
				Assert.Equal("test/2", readKey.Item1);
				Assert.Equal("3", readKey.Item2);

			}
		}


		[Fact]
		public void CanDelete()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test"));

				tx.State.Root.Delete(tx, "test");
				Assert.Null(ReadKey(tx, "test"));
			}
		}

		[Fact]
		public void CanDelete2()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test/1", StreamFor("1"));
				tx.State.Root.Add(tx, "test/2", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test/2"));

				tx.State.Root.Delete(tx, "test/2");
				Assert.Null(ReadKey(tx, "test/2"));
				Assert.NotNull(ReadKey(tx, "test/1"));
			}
		}

		[Fact]
		public void CanDelete1()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "test/1", StreamFor("1"));
				tx.State.Root.Add(tx, "test/2", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test/1"));

				tx.State.Root.Delete(tx, "test/1");
				Assert.Null(ReadKey(tx, "test/1"));
				Assert.NotNull(ReadKey(tx, "test/2"));
			}
		}
	}
}