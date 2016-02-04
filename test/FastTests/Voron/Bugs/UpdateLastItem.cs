using Xunit;
using Voron;
using Voron.Data.BTrees;

namespace FastTests.Voron.Bugs
{
	public unsafe class UpdateLastItem : StorageTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var tx = Env.WriteTransaction())
			{
			    var tree = tx.CreateTree("t");
				tree.DirectAdd((Slice) "events", sizeof (TreeRootHeader));
				tree.DirectAdd((Slice) "aggregations", sizeof(TreeRootHeader));
				tree.DirectAdd((Slice) "aggregation-status", sizeof(TreeRootHeader));
				tx.Commit();
			}
			using (var tx = Env.WriteTransaction())
			{
                var tree = tx.CreateTree("t");
                tree.DirectAdd((Slice) "events", sizeof(TreeRootHeader));

				tx.Commit();
			}

			RestartDatabase();

			using (var tx = Env.WriteTransaction())
			{
                var tree = tx.CreateTree("t");
                tree.DirectAdd((Slice) "events", sizeof(TreeRootHeader));

				tx.Commit();
			}
		}
	}
}