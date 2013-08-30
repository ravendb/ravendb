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
			using (var pager = new PureMemoryPager())
			{
				using (var env = new StorageEnvironment(pager, false))
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.DirectAdd(tx, "events", sizeof (TreeRootHeader));
						env.Root.DirectAdd(tx, "aggregations", sizeof (TreeRootHeader));
						env.Root.DirectAdd(tx, "aggregation-status", sizeof (TreeRootHeader));
						tx.Commit();
					}
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.DirectAdd(tx, "events", sizeof (TreeRootHeader));

						tx.Commit();
					}
				}

				using (var env = new StorageEnvironment(pager, false))
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.Root.DirectAdd(tx, "events", sizeof(TreeRootHeader));

					tx.Commit();
				}
			}
		}
	}
}