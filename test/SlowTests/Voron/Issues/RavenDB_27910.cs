using System.Diagnostics;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Util.Conversion;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public unsafe class RavenDB_27910(ITestOutputHelper output) : StorageTest(output)
{
    // room between consecutive keys, so we can append to the end of a leaf that is not the last one in the tree
    private const long Stride = 10_000;

    [RavenFact(RavenTestCategory.Voron)]
    public void EstimatedCountWhenLeafPrecedesBranch()
    {
        Slice.From(Allocator, "etags", out var treeName);

        long numberOfEntries = 0;

        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: sizeof(long));

            // sequential inserts until the root splits into [A, B], both branches
            long i = 0;
            while (fst.Depth < 3)
            {
                Add(fst, i * Stride);
                i++;
            }

            var root = GetRoot(fst);
            Assert.Equal(2, root.NumberOfEntries);
            var a = fst.GetReadOnlyPage(root.GetEntry(0)->PageNumber);
            Assert.True(a.IsBranch);

            // append to the end of A's last leaf until A splits, a full branch split at its end leaves a branch with 2 children: [A, A2, B]
            var aLastLeaf = fst.GetReadOnlyPage(a.GetEntry(a.NumberOfEntries - 1)->PageNumber);
            var key = aLastLeaf.GetKey(aLastLeaf.NumberOfEntries - 1);
            while (GetRoot(fst).NumberOfEntries == 2)
            {
                key++;
                Assert.True(key % Stride != 0, "ran out of room between keys");
                Add(fst, key);
            }

            root = GetRoot(fst);
            Assert.Equal(3, root.NumberOfEntries);
            var a2 = fst.GetReadOnlyPage(root.GetEntry(1)->PageNumber);
            Assert.True(a2.IsBranch);
            Assert.Equal(2, a2.NumberOfEntries);

            // deleting the only entry of A2's last leaf leaves A2 with one child, so it collapses into that leaf: [A, leaf, B]
            var a2LastLeaf = fst.GetReadOnlyPage(a2.GetEntry(1)->PageNumber);
            Assert.Equal(1, a2LastLeaf.NumberOfEntries);
            Assert.Equal(key, a2LastLeaf.GetKey(0));
            fst.Delete(key);

            root = GetRoot(fst);
            Assert.Equal(3, root.NumberOfEntries);
            Assert.True(fst.GetReadOnlyPage(root.GetEntry(0)->PageNumber).IsBranch);
            Assert.True(fst.GetReadOnlyPage(root.GetEntry(1)->PageNumber).IsLeaf);
            Assert.True(fst.GetReadOnlyPage(root.GetEntry(2)->PageNumber).IsBranch);

            numberOfEntries = fst.NumberOfEntries;

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: sizeof(long));

            // starts in A, the estimation samples the sibling to the left of B, which is the collapsed leaf
            var exact = fst.GetNumberOfEntriesAfter(Stride, out var totalCount, Stopwatch.StartNew(), EstimationAccuracy.Exact);
            Assert.Equal(numberOfEntries, totalCount);
            Assert.Equal(numberOfEntries - 2, exact);

            var estimated = fst.GetNumberOfEntriesAfter(Stride, out totalCount, Stopwatch.StartNew(), EstimationAccuracy.Estimated);
            Assert.Equal(numberOfEntries, totalCount);
            Assert.InRange(estimated, 1, totalCount);
        }
    }

    private static void Add(FixedSizeTree fst, long key)
    {
        // like a table's etag index, the value is a storage id, which is far beyond the number of allocated pages
        var storageId = key * Constants.Storage.PageSize;
        fst.Add(key, EndianBitConverter.Little.GetBytes(storageId));
    }

    private static FixedSizeTreePage<long> GetRoot(FixedSizeTree fst)
    {
        var header = (FixedSizeTreeHeader.Large*)fst.Parent.DirectRead(fst.Name);
        return fst.GetReadOnlyPage(header->RootPageNumber);
    }
}
