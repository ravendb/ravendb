using System.IO;
using Nevar.Impl;
using Xunit;

namespace Nevar.Tests.Trees
{
	public class Updates : StorageTest
	{
		[Fact]
		public void UpdateThatIsBiggerThanPageSize()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "1", new MemoryStream(new byte[1200]));
				Env.Root.Add(tx, "2", new MemoryStream(new byte[1200]));
				Env.Root.Add(tx, "3", new MemoryStream(new byte[1200]));

				tx.Commit();
			}

			// update that is too big
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "1", new MemoryStream(new byte[Constants.MaxNodeSize - 10]));

				tx.Commit();
			}

			Assert.Equal(3 , Env.Root.PageCount);
			Assert.Equal(0, Env.Root.OverflowPages);
		}

		[Fact]
		public void CanAddAndUpdate()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test", StreamFor("1"));
				Env.Root.Add(tx, "test", StreamFor("2"));

				var readKey = ReadKey(tx, "test");
				Assert.Equal("test", readKey.Item1);
				Assert.Equal("2", readKey.Item2);
			}
		}

		[Fact]
		public void CanAddAndUpdate2()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test/1", StreamFor("1"));
				Env.Root.Add(tx, "test/2", StreamFor("2"));
				Env.Root.Add(tx, "test/1", StreamFor("3"));

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
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test/1", StreamFor("1"));
				Env.Root.Add(tx, "test/2", StreamFor("2"));
				Env.Root.Add(tx, "test/2", StreamFor("3"));

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
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test"));

				Env.Root.Delete(tx, "test");
				Assert.Null(ReadKey(tx, "test"));
			}
		}

		[Fact]
		public void CanDelete2()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test/1", StreamFor("1"));
				Env.Root.Add(tx, "test/2", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test/2"));

				Env.Root.Delete(tx, "test/2");
				Assert.Null(ReadKey(tx, "test/2"));
				Assert.NotNull(ReadKey(tx, "test/1"));
			}
		}

		[Fact]
		public void CanDelete1()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test/1", StreamFor("1"));
				Env.Root.Add(tx, "test/2", StreamFor("1"));
				Assert.NotNull(ReadKey(tx, "test/1"));

				Env.Root.Delete(tx, "test/1");
				Assert.Null(ReadKey(tx, "test/1"));
				Assert.NotNull(ReadKey(tx, "test/2"));
			}
		}
	}
}