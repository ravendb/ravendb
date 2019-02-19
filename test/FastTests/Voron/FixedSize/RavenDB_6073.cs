using System.Collections.Generic;
using Voron;
using Xunit;
using Constants = Voron.Global.Constants;

namespace FastTests.Voron.FixedSize
{
    public class RavenDB_6073 : StorageTest
    {
        [Fact]
        public void Branch_page_collapsing_during_tree_rebalancing()
        {
            var numberOfItems = 10000;
            ushort valueSize = Constants.Storage.PageSize/16;

            Slice itemsTree;

            using (Slice.From(Allocator, "items-tree", out itemsTree))
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(itemsTree, valueSize);

                var bytes = new byte[valueSize];

                var keysToRemove = new List<long>();

                var insertCount = 0;

                Slice val;
                using (Slice.From(Allocator, bytes, out val))
                {
                    for (var i = 0; i < numberOfItems; i++)
                    {
                        fst.Add(i, val);

                        insertCount++;

                        if (fst.Depth != 3)
                            continue;

                        foreach (var pageNumber in fst.AllPages())
                        {
                            var page = fst.GetReadOnlyPage(pageNumber);

                            if (page.IsLeaf && page.NumberOfEntries > 1)
                            {
                                for (int j = 0; j < page.NumberOfEntries; j++)
                                {
                                    var key = page.GetKey(j);
                                    keysToRemove.Add(key);
                                }
                                    
                                break;
                            }
                        }

                        break;
                    }
                }

                Assert.True(fst.Depth >= 3);
                Assert.NotNull(keysToRemove);

                foreach (var key in keysToRemove)
                {
                    fst.Delete(key);
                }

                for (int i = 0; i < insertCount; i++)
                {
                    Slice value;
                    using (fst.Read(i, out value))
                    {
                        if (keysToRemove.Contains(i))
                            Assert.False(value.HasValue);
                        else
                            Assert.True(value.HasValue);
                    }
                }
            }
        }
    }
}