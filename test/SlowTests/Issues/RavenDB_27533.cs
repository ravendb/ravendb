using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27533(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void SplittingLeafNextToBranchSiblingMustWrapItInBranch()
    {
        const long stride = 1L << 40;

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>("entries");
            long lastKey = 0;
            while (lookup.State.BranchPages < 3)
            {
                lastKey += stride;
                lookup.Add(lastKey, lastKey);
            }

            Assert.Single(LeafDepths(lookup));
            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));
            long leafPages = lookup.State.LeafPages;

            // Empties the single-entry leaf. FreePageFor collapses its two-entry parent into a leaf directly under the root.
            Assert.True(lookup.TryRemove(lastKey, out _));
            Assert.Equal(2, lookup.State.BranchPages);
            Assert.Equal(new[] { Branch, Leaf }, RootChildPageFlags(lookup));
            Assert.Equal(leafPages - 1, lookup.State.LeafPages);
            Assert.Equal(1, ReadEdgeDepth(lookup, right: true));
            Assert.Equal(2, ReadEdgeDepth(lookup, right: false));

            // The collapsed leaf is full, so this append splits it. Its sibling under the root is a branch,
            // so the split wraps the leaf in a new branch instead of adding a leaf pointer to the root.
            lookup.Add(lastKey, lastKey);
            Assert.Equal(3, lookup.State.BranchPages);
            Assert.Equal(leafPages, lookup.State.LeafPages);
            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));
            Assert.Single(LeafDepths(lookup));
            Assert.True(lookup.TryGetValue(lastKey, out long value));
            Assert.Equal(lastKey, value);
            lookup.VerifyStructure();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WrappedBranchMustCollapseUnderLifoDeletesAndWrapAgain()
    {
        const long stride = 1L << 40;
        const string lookupName = "entries";

        long next = stride;
        long count = 0;

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>(lookupName);

            // Root split: Root = [B1, B2], B2 = [stolen full leaf, leaf holding only the last key].
            while (lookup.State.BranchPages < 3)
                Add(lookup);

            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));

            // Emptying the last leaf collapses B2 into a leaf: Root = [B1, leaf].
            RemoveTop(lookup);
            Assert.Equal(2, lookup.State.BranchPages);
            Assert.Equal(new[] { Branch, Leaf }, RootChildPageFlags(lookup));

            // The leaf is full, so the add splits it and wraps it in place: Root = [B1, B2'] again.
            Add(lookup);
            Assert.Equal(3, lookup.State.BranchPages);
            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));

            long leafPages = lookup.State.LeafPages;
            while (lookup.State.LeafPages < leafPages + 2)
                Add(lookup);

            Assert.Equal(new[] { Leaf, Leaf, Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));

            // Each emptied leaf is removed from B2', the last one collapses it back into a leaf.
            while (lookup.State.BranchPages == 3)
                RemoveTop(lookup);

            Assert.Equal(new[] { Branch, Leaf }, RootChildPageFlags(lookup));
            Assert.Equal(1, ReadEdgeDepth(lookup, right: true));
            Assert.Equal(2, ReadEdgeDepth(lookup, right: false));
            Assert.Equal(count, lookup.NumberOfEntries);
            lookup.VerifyStructure();

            while (lookup.State.BranchPages < 3)
                Add(lookup);

            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));
            Assert.Single(LeafDepths(lookup));
            Assert.Equal(count, lookup.NumberOfEntries);
            lookup.VerifyStructure();

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>(lookupName);
            Assert.Equal(count, lookup.NumberOfEntries);

            long expected = stride;
            var it = lookup.Iterate();
            it.Reset();
            while (it.MoveNext(out long value))
            {
                Assert.Equal(expected, value);
                expected += stride;
            }

            Assert.Equal(next, expected);
        }

        void Add(Lookup<Int64LookupKey> lookup)
        {
            lookup.Add(next, next);
            next += stride;
            count++;
        }

        void RemoveTop(Lookup<Int64LookupKey> lookup)
        {
            next -= stride;
            Assert.True(lookup.TryRemove(next, out _));
            count--;
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AddIntoLeafNextToBranchUnderRootMustTurnItIntoBranch()
    {
        const long stride = 1L << 40;

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>("entries");

            long lastKey = 0;
            while (lookup.State.BranchPages < 3)
            {
                lastKey += stride;
                lookup.Add(lastKey, lastKey);
            }

            // Removing the only key of the newest leaf collapses its two-entry parent into a leaf: Root = [branch, leaf].
            Assert.True(lookup.TryRemove(lastKey, out _));

            long rootPage = lookup.State.RootPage;
            Assert.Equal(new[] { Branch, Leaf }, ChildPageFlags(lookup, rootPage));
            long leafPage = lookup.AllEntriesIn(rootPage)[1].Item2;
            long leafPages = lookup.State.LeafPages;

            // The leaf is full, so this add splits it. Instead of adding a leaf pointer to the root,
            // the leaf page itself becomes a branch over two leaves: Root = [branch, branch], same page numbers.
            lookup.Add(lastKey, lastKey);

            Assert.Equal(new[] { Branch, Branch }, ChildPageFlags(lookup, rootPage));
            Assert.Equal(leafPage, lookup.AllEntriesIn(rootPage)[1].Item2);
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, leafPage));
            Assert.Equal(3, lookup.State.BranchPages);
            Assert.Equal(leafPages + 1, lookup.State.LeafPages);
            Assert.Single(LeafDepths(lookup));
            Assert.True(lookup.TryGetValue(lastKey, out long value));
            Assert.Equal(lastKey, value);
            lookup.VerifyStructure();
        }
    }

    private const LookupPageFlags Branch = LookupPageFlags.Branch;
    private const LookupPageFlags Leaf = LookupPageFlags.Leaf;

    private static LookupPageFlags[] RootChildPageFlags(Lookup<Int64LookupKey> lookup)
    {
        return ChildPageFlags(lookup, lookup.State.RootPage);
    }

    private static long LastRootChild(Lookup<Int64LookupKey> lookup)
    {
        return lookup.AllEntriesIn(lookup.State.RootPage)[^1].Item2;
    }

    private static unsafe LookupPageFlags[] ChildPageFlags(Lookup<Int64LookupKey> lookup, long pageNumber)
    {
        return lookup.AllEntriesIn(pageNumber)
            .Select(x => ((LookupPageHeader*)lookup.Llt.GetPage(x.Item2).Pointer)->PageFlags)
            .ToArray();
    }

    private static int[] LeafDepths(Lookup<Int64LookupKey> lookup)
    {
        return LeafDepths(lookup, lookup.State.RootPage, 1);
    }

    private static unsafe int[] LeafDepths(Lookup<Int64LookupKey> lookup, long pageNumber, int depth)
    {
        var header = (LookupPageHeader*)lookup.Llt.GetPage(pageNumber).Pointer;
        if (header->IsLeaf)
            return new[] { depth };

        return lookup.AllEntriesIn(pageNumber).SelectMany(x => LeafDepths(lookup, x.Item2, depth + 1)).Distinct().ToArray();
    }

    private static unsafe int ReadEdgeDepth(
        Lookup<Int64LookupKey> lookup,
        bool right)
    {
        long pageNumber = lookup.State.RootPage;
        int depth = 0;

        while (true)
        {
            var header =
                (LookupPageHeader*)lookup.Llt.GetPage(pageNumber).Pointer;

            if (header->IsLeaf)
                return depth;

            Assert.True(header->IsBranch);

            var children = lookup.AllEntriesIn(pageNumber);
            Assert.NotEmpty(children);

            pageNumber =
                children[right ? children.Count - 1 : 0].Item2;

            depth++;

            Assert.True(
                depth <= 64,
                "Unexpected excessive depth during inspection.");
        }
    }
}
