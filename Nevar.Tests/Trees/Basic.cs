using System;
using System.IO;
using Xunit;

namespace Nevar.Tests.Trees
{
	public unsafe class Basic : StorageTest
	{
		[Fact]
		public void CanAdd()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test", StreamFor("value"));
			}
		}

		[Fact]
		public void CanAddAndRead()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "b", StreamFor("2"));
				Env.Root.Add(tx, "c", StreamFor("3"));
				Env.Root.Add(tx, "a", StreamFor("1"));
				var actual = ReadKey(tx, "a");

				Assert.Equal("a", actual.Item1);
				Assert.Equal("1", actual.Item2);
			}
		}

		private Tuple<Slice, Slice> ReadKey(Transaction tx, Slice key)
		{
			var cursor = tx.GetCursor(Env.Root);
			var p = Env.Root.FindPageFor(tx, key, cursor);
			bool match;
			var node = p.Search(key, Env.SliceComparer, out match);

			Assert.True(node != null);

			return Tuple.Create(new Slice(node),
								new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort)node->DataSize));
		}

		[Fact]
		public void CanAddAndReadStats()
		{
			using (var tx = Env.NewTransaction())
			{
				Slice key = "test";
				Env.Root.Add(tx, key, StreamFor("value"));
				Assert.Equal(1, Env.Root.PageCount);
				Assert.Equal(1, Env.Root.LeafPages);
			}
		}

		[Fact]
		public void CanAddEnoughToCausePageSplit()
		{
			using (var tx = Env.NewTransaction())
			{
				Stream stream = StreamFor("value");

				for (int i = 0; i < 256; i++)
				{
					stream.Position = 0;
					Env.Root.Add(tx, "test-" + i, stream);

				}

				tx.Commit();

				Assert.Equal(3, Env.Root.PageCount);
				Assert.Equal(2, Env.Root.LeafPages);
				Assert.Equal(1, Env.Root.BranchPages);
				Assert.Equal(2, Env.Root.Depth);

			}
		}

		[Fact]
		public void AfterPageSplitAllDataIsValid()
		{
			using (var tx = Env.NewTransaction())
			{
				for (int i = 0; i < 256; i++)
				{
					Env.Root.Add(tx, "test-" + i, StreamFor("val-" + i));
				}

				tx.Commit();
			}
			using (var tx = Env.NewTransaction())
			{
				for (int i = 0; i < 256; i++)
				{
					var read = ReadKey(tx, "test-" + i);
					Assert.Equal("test-" + i, read.Item1);
					Assert.Equal("val-" + i, read.Item2);

				}
			}
		}

		[Fact]
		public void PageSplitsAllAround()
		{

			using (var tx = Env.NewTransaction())
			{
				Stream stream = StreamFor("value");

				for (int i = 0; i < 256; i++)
				{
					for (int j = 0; j < 5; j++)
					{
						stream.Position = 0;
						Env.Root.Add(tx, "test-" + j + i, stream);

					}
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction())
			{
				RenderAndShow(tx, Env.Root);
			}

			Assert.False(true);


		}
	}
}