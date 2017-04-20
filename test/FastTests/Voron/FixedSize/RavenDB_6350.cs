using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Global;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class RavenDB_6350 : StorageTest
    {
        [Fact]
        public void Invalid_usage_of_DirectAdds()
        {
            var numberOfItems = 100;
            ushort valueSize = Constants.Storage.PageSize / 16;

            Slice fstName;

            using (Slice.From(Allocator, "ccc", out fstName))
            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("parent");

                var allocator = new NewPageAllocator(tx.LowLevelTransaction, parent);
                allocator.Create();

                for (int i = 0; i < 6; i++)
                {
                    parent.Add($"aaaaa-{i}", new byte[1000]);
                }

                parent.Add($"dummy-8", new byte[1300]);

                for (int i = 0; i < 6; i++)
                {
                    parent.Delete($"aaaaa-{i}");
                }
                
                for (int i = 0; i < NewPageAllocator.NumberOfPagesInSection - 1; i++)
                {
                    allocator.AllocateSinglePage(0);
                }
                
                var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, fstName, valueSize, newPageAllocator: allocator);

                var bytes = new byte[valueSize];

                Slice val;
                using (Slice.From(Allocator, bytes, out val))
                {
                    for (var i = 0; i < numberOfItems; i++)
                    {
                        fst.Add(i, val);
                    }
                }

                tx.Commit();
            }
        }
    }
}