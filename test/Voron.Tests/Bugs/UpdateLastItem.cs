using System.IO;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Xunit;

namespace Voron.Tests.Bugs
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