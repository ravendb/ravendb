using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
	public class Basic : StorageTest
	{

		[Fact]
        public void CanAddVeryLargeValue()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            List<long> allPages = null;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.Root.Add(tx, "a", new MemoryStream(buffer));
                allPages = Env.Root.AllPages(tx);
                tx.Commit();
            }

            Assert.Equal(Env.Root.State.PageCount, allPages.Count);
            Assert.Equal(4, Env.Root.State.PageCount);
            Assert.Equal(3, Env.Root.State.OverflowPages);
        }

		[Fact]
		public void CanAdd()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.Root.Add(tx, "test", StreamFor("value"));
			}
		}

		[Fact]
		public void CanAddAndRead()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.Root.Add(tx, "b", StreamFor("2"));
				Env.Root.Add(tx, "c", StreamFor("3"));
				Env.Root.Add(tx, "a", StreamFor("1"));
				var actual = ReadKey(tx, "a");

				Assert.Equal("a", actual.Item1);
				Assert.Equal("1", actual.Item2);
			}
		}

		[Fact]
		public void CanAddAndReadStats()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Slice key = "test";
				Env.Root.Add(tx, key, StreamFor("value"));
                Assert.Equal(1, Env.Root.State.PageCount);
                Assert.Equal(1, Env.Root.State.LeafPages);
			}
		}

		[Fact]
		public void CanAddEnoughToCausePageSplit()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Stream stream = StreamFor("value");

				for (int i = 0; i < 256; i++)
				{
					stream.Position = 0;
					Env.Root.Add(tx, "test-" + i, stream);

				}

				tx.Commit();
// ReSharper disable ConditionIsAlwaysTrueOrFalse
				if (tx.Environment.PageSize != 4096)
					return;
// ReSharper restore ConditionIsAlwaysTrueOrFalse
                Assert.Equal(4, Env.Root.State.PageCount);
                Assert.Equal(3, Env.Root.State.LeafPages);
                Assert.Equal(1, Env.Root.State.BranchPages);
                Assert.Equal(2, Env.Root.State.Depth);

			}
		}

		[Fact]
		public void AfterPageSplitAllDataIsValid()
		{
			const int count = 256;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < count; i++)
				{
					Env.Root.Add(tx, "test-" + i.ToString("000"), StreamFor("val-" + i));
					
				}

				tx.Commit();
			}
            using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < count; i++)
				{
					var read = ReadKey(tx, "test-" + i.ToString("000"));
					Assert.Equal("test-" + i.ToString("000"), read.Item1);
					Assert.Equal("val-" + i, read.Item2);
				}
			}
		}

		[Fact]
		public void PageSplitsAllAround()
		{
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Stream stream = StreamFor("value");

				for (int i = 0; i < 256; i++)
				{
					for (int j = 0; j < 5; j++)
					{
						stream.Position = 0;
						if (j == 1 && i == 65)
						{
							
						}
						Env.Root.Add(tx, "test-" + j.ToString("000") + "-" + i.ToString("000"), stream);
					}
				}

				tx.Commit();
			}

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				for (int i = 0; i < 256; i++)
				{
					for (int j = 0; j < 5; j++)
					{
						var key = "test-" + j.ToString("000") + "-" + i.ToString("000");
						var readKey = ReadKey(tx, key);
						Assert.Equal(readKey.Item1, key);
					}
			}
			}
		}
	}
}
