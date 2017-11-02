using Voron;
using Voron.Data.Fixed;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_9225 : StorageTest
    {
        [Fact]
        public unsafe void FixedSizeTree_DeletingFromMiddle()
        {
            FixedSizeTreeHeader.Large* header;
            FixedSizeTreePage page;
            ushort valSize = 1008;
            var buffer = new byte[valSize];
            using (var tx = Env.WriteTransaction())
            using (Slice.From(tx.Allocator, "test", out var t))
            {
                var fst = tx.FixedTreeFor(t, valSize);

                long i = 0;
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

                var min = page.GetEntry(0)->Key;
                var max = page.GetEntry(page.NumberOfEntries - 1)->Key;

                for (i = min; i < max; i++)
                    fst.Delete(i);
                fst.Delete(max);
                fst.ValidateTree_Forced();
            }
        }
    }
}
