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
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.DirectAdd((Slice) "events", sizeof (TreeRootHeader));
				tx.State.Root.DirectAdd((Slice) "aggregations", sizeof(TreeRootHeader));
				tx.State.Root.DirectAdd((Slice) "aggregation-status", sizeof(TreeRootHeader));
				tx.Commit();
			}
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.DirectAdd((Slice) "events", sizeof(TreeRootHeader));

				tx.Commit();
			}

			RestartDatabase();

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.DirectAdd((Slice) "events", sizeof(TreeRootHeader));

				tx.Commit();
			}
		}
	}
}