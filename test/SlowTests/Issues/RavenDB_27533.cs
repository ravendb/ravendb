using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data;
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

            // Empties the single-entry leaf. FreePageFor collapses its two-entry parent into a leaf directly
            // under the root, marking it with CollapsedLevels = 1.
            Assert.True(lookup.TryRemove(lastKey, out _));
            Assert.Equal(2, lookup.State.BranchPages);
            Assert.Equal(new[] { Branch, CollapsedLeaf }, RootChildPageFlags(lookup));
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
            Assert.Equal(new[] { Branch, CollapsedLeaf }, RootChildPageFlags(lookup));

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

            Assert.Equal(new[] { Branch, CollapsedLeaf }, RootChildPageFlags(lookup));
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
            Assert.Equal(new[] { Branch, CollapsedLeaf }, ChildPageFlags(lookup, rootPage));
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

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void UnmarkedLeafFromOlderVersionNextToBranchSiblingIsStillWrapped()
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

            Assert.True(lookup.TryRemove(lastKey, out _));
            Assert.Equal(new[] { Branch, CollapsedLeaf }, RootChildPageFlags(lookup));

            // pages written before the CollapsedLevels counter existed aren't marked, the sibling
            // probes in ShouldPromoteLeaf must still detect the branch sibling and wrap
            long leafPage = lookup.AllEntriesIn(lookup.State.RootPage)[1].Item2;
            var page = lookup.Llt.ModifyPage(leafPage);
            ((LookupPageHeader*)page.Pointer)->PageFlags = Leaf;
            Assert.Equal(new[] { Branch, Leaf }, RootChildPageFlags(lookup));

            lookup.Add(lastKey, lastKey);

            Assert.Equal(3, lookup.State.BranchPages);
            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, LastRootChild(lookup)));
            Assert.Single(LeafDepths(lookup));
            lookup.VerifyStructure();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void CollapsingBranchIntoBranchMarksItAndItsNextSplitWrapsIt()
    {
        const long stride = 1L << 40;

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>("entries");

            long next = stride;
            while (lookup.State.BranchPages < 3)
            {
                lookup.Add(next, next);
                next += stride;
            }

            // collapse the newest branch into a leaf, re-adding splits the full leaf and wraps it:
            // Root = [B1, W], W = [leaf, leaf]
            next -= stride;
            Assert.True(lookup.TryRemove(next, out _));
            lookup.Add(next, next);
            next += stride;

            long rootPage = lookup.State.RootPage;
            long wPage = lookup.AllEntriesIn(rootPage)[1].Item2;
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, wPage));

            // pretend the newest leaf under W is a collapse remnant, so that its split builds a branch
            // under the non-root W - a structure that otherwise requires a four-level tree
            long nPage = lookup.AllEntriesIn(wPage)[1].Item2;
            ((LookupPageHeader*)lookup.Llt.ModifyPage(nPage).Pointer)->CollapsedLevels = 1;

            while (lookup.State.BranchPages < 4)
            {
                lookup.Add(next, next);
                next += stride;
            }

            Assert.Equal(new[] { Leaf, Branch }, ChildPageFlags(lookup, wPage));

            // emptying W's leftmost leaf copies the branch sibling over it, then the two-entry W
            // collapses into that branch content - a branch that must carry CollapsedLevels = 1
            long c0Page = lookup.AllEntriesIn(wPage)[0].Item2;
            foreach (var (key, _) in lookup.AllEntriesIn(c0Page))
                Assert.True(lookup.TryRemove(key.ToLong(), out _));

            Assert.Equal(new[] { Branch, CollapsedBranch }, RootChildPageFlags(lookup));
            Assert.Equal(wPage, lookup.AllEntriesIn(rootPage)[1].Item2);
            lookup.VerifyStructure();

            // when the marked branch fills up and splits, it must wrap in place
            // instead of adding another pointer to the root
            var wHeader = (LookupPageHeader*)lookup.Llt.GetPage(wPage).Pointer;
            while (wHeader->CollapsedLevels > 0)
            {
                lookup.Add(next, next);
                next += stride;
                wHeader = (LookupPageHeader*)lookup.Llt.GetPage(wPage).Pointer;
            }

            Assert.Equal(new[] { Branch, Branch }, RootChildPageFlags(lookup));
            Assert.Equal(wPage, lookup.AllEntriesIn(rootPage)[1].Item2);
            Assert.Equal(new[] { Branch, Branch }, ChildPageFlags(lookup, wPage));
            Assert.True(lookup.TryGetValue(next - stride, out long value));
            Assert.Equal(next - stride, value);
            lookup.VerifyStructure();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void TwiceCollapsedPageMustWrapOnTwoConsecutiveSplits()
    {
        const long stride = 1L << 40;

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>("entries");

            long next = stride;
            while (lookup.State.BranchPages < 3)
            {
                lookup.Add(next, next);
                next += stride;
            }

            // collapse the newest branch into a leaf, re-adding splits the full leaf and wraps it:
            // Root = [B1, W], W = [leaf, leaf]
            next -= stride;
            Assert.True(lookup.TryRemove(next, out _));
            lookup.Add(next, next);
            next += stride;

            long wPage = lookup.AllEntriesIn(lookup.State.RootPage)[1].Item2;
            long nPage = lookup.AllEntriesIn(wPage)[1].Item2;

            // a page that collapsed twice between its splits is two levels shallower than its siblings
            ((LookupPageHeader*)lookup.Llt.ModifyPage(nPage).Pointer)->CollapsedLevels = 2;

            // the first split wraps in place, and the wrapper still owes one level
            while (lookup.State.BranchPages < 4)
            {
                lookup.Add(next, next);
                next += stride;
            }

            var nHeader = (LookupPageHeader*)lookup.Llt.GetPage(nPage).Pointer;
            Assert.True(nHeader->IsBranch);
            Assert.Equal(1, nHeader->CollapsedLevels);
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(lookup, nPage));

            // when the wrapper fills up, its own split wraps again, fully restoring the levels
            while (nHeader->CollapsedLevels > 0)
            {
                lookup.Add(next, next);
                next += stride;
                nHeader = (LookupPageHeader*)lookup.Llt.GetPage(nPage).Pointer;
            }

            Assert.True(nHeader->IsBranch);
            Assert.Equal(new[] { Branch, Branch }, ChildPageFlags(lookup, nPage));
            Assert.Equal(new[] { Leaf, Branch }, ChildPageFlags(lookup, wPage));
            Assert.True(lookup.TryGetValue(next - stride, out long value));
            Assert.Equal(next - stride, value);
            lookup.VerifyStructure();
        }
    }

    private const LookupPageFlags Branch = LookupPageFlags.Branch;
    private const LookupPageFlags Leaf = LookupPageFlags.Leaf;
    private static readonly LookupPageFlags CollapsedLeaf = PageCollapsedLevels.Set(LookupPageFlags.Leaf, 1);
    private static readonly LookupPageFlags CollapsedBranch = PageCollapsedLevels.Set(LookupPageFlags.Branch, 1);

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
