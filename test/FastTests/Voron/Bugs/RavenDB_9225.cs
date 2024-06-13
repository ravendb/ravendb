using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_9225 : StorageTest
    {
        public RavenDB_9225(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void VariableSizeTree_DeletingFromMiddle()
        {
                        TreePage page;
            ushort valSize = 1008;
            var buffer = new byte[valSize];
            using (var tx = Env.WriteTransaction())
            using (Slice.From(tx.Allocator, "test", out var t))
            {
                var tree = tx.CreateTree(t);

                long i = 0;
                while (true)
                {
                    tree.Add(i.ToString("D19"), buffer);
                    if (tree.State.Header.Depth == 3)
                    {
                        page = tree.GetReadOnlyTreePage(tree.State.Header.RootPageNumber);
                        if (page.NumberOfEntries == 3)
                        {
                            page = tree.GetReadOnlyTreePage(page.GetNode(page.NumberOfEntries - 1)->PageNumber);
                            if (page.NumberOfEntries == 3)
                                break;
                        }
                    }
                    i++;
                }


                page = tree.GetReadOnlyTreePage(tree.State.Header.RootPageNumber);
                page = tree.GetReadOnlyTreePage(page.GetNode(page.NumberOfEntries - 1)->PageNumber);
                page = tree.GetReadOnlyTreePage(page.GetNode(0)->PageNumber);

                var pageNumberOfEntries = page.NumberOfEntries;

                for (i = 0; i < pageNumberOfEntries; i++)
                {
                    using (page.GetNodeKey(tx.LowLevelTransaction, 0, out var key))
                        tree.Delete(key);
                }
                using (page.GetNodeKey(tx.LowLevelTransaction, 0, out var key))
                    tree.Delete(key);
                tree.ValidateTree_Forced(tree.State.Header.RootPageNumber);
            }
        }

        [Fact]
        public unsafe void FixedSizeTree_DeletingFromMiddle()
        {
            ushort valSize = 1008;
            var buffer = new byte[valSize];
            using (var tx = Env.WriteTransaction())
            using (Slice.From(tx.Allocator, "test", out var t))
            {
                var fst = tx.FixedTreeFor(t, valSize);

                long i = 0;
                FixedSizeTreeHeader.Large* header;
                FixedSizeTreePage<long> page;
                while (true)
                {
                    fst.Add(++i, buffer);
                    if (fst.Depth == 3)
                    {
                        header = (FixedSizeTreeHeader.Large*)tx.LowLevelTransaction.RootObjects.DirectRead(t);
                        page = fst.GetReadOnlyPage(header->RootPageNumber);
                        if (page.NumberOfEntries == 3)
                        {
                            page = fst.GetReadOnlyPage(page.GetEntry(page.NumberOfEntries - 1)->PageNumber);
                            if (page.NumberOfEntries == 3)
                                break;
                        }
                    }
                }


                header = (FixedSizeTreeHeader.Large*)tx.LowLevelTransaction.RootObjects.DirectRead(t);
                page = fst.GetReadOnlyPage(header->RootPageNumber);
                page = fst.GetReadOnlyPage(page.GetEntry(page.NumberOfEntries - 1)->PageNumber);
                page = fst.GetReadOnlyPage(page.GetEntry(0)->PageNumber);

                var min = page.GetEntry(0)->GetKey<long>();
                var max = page.GetEntry(page.NumberOfEntries - 1)->GetKey<long>();

                for (i = min; i < max; i++)
                    fst.Delete(i);
                fst.Delete(max);
                fst.ValidateTree_Forced();
            }
        }
    }
}
