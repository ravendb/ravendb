using System.Threading.Tasks;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25947(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public async Task CanRecordAllFreedPages()
        {
            using (var tx = Env.WriteTransaction())
            {
                var allocator = new NewPageAllocator(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects);

                var tree = tx.CreateTree("test", isIndexTree: true, newPageAllocator: allocator);
                var data = new string('x', 2048);
                for (int i = 0; i < 4000; i++)
                {
                    tree.Add($"key/{i:D9}", data);
                }

                int x = 0;
                for (int i = 0; i < 15; i+=2)
                {
                    // disjoint frees, so we keep them in the allocator 
                    x += 100;
                    for (int j = 0; j < 50; j++)
                    {
                        x++;
                        tree.Delete($"key/{x:D9}");
                    }
                }

                var pages = allocator.AllPages();
                Assert.True(pages.Count > 256);
            }
        }

    }
}
