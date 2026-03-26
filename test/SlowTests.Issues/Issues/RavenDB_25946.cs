using System.Threading.Tasks;
using Amqp.Framing;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25946(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public async Task AllocatorWillFreePagesBackToGlobalFree()
        {
            var data = new string('x', 2048);
            using (var tx = Env.WriteTransaction())
            {
                var allocator = new NewPageAllocator(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects);

                var tree = tx.CreateTree("test", isIndexTree: true, newPageAllocator: allocator);
                for (int i = 0; i < 6000; i++)
                {
                    tree.Add($"key/{i:D9}", data);
                }

                Assert.Equal(0, Env.FreeSpaceHandling.GetFreePagesCount(tx.LowLevelTransaction));

                for (int i = 500; i < 6000; i++)
                {
                    tree.Delete($"key/{i:D9}");
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(1792, Env.FreeSpaceHandling.GetFreePagesCount(tx.LowLevelTransaction));

                var allocator = new NewPageAllocator(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects);
                Assert.NotEmpty(allocator.AllPages());

                var tree = tx.CreateTree("test", isIndexTree: true, newPageAllocator: allocator);

                for (int i = 6000; i < 9000; i++)
                {
                    tree.Add($"key/{i}", data);
                }
                tx.Commit();
            }
        }

    }
}
