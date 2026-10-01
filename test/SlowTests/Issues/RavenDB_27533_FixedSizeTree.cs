using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Impl;
using Xunit;

namespace SlowTests.Issues;

public unsafe class RavenDB_27533_FixedSizeTree(ITestOutputHelper output) : StorageTest(output)
{
    private const int Stride = 16;
    private static readonly byte[] Value = new byte[8];

    /// <summary>
    /// The collapse that RavenDB-27533 is about needs a branch that is down to a single child, which a
    /// fixed size tree only reaches for the root, because the rebalancer merges or steals from a sibling
    /// long before a branch gets that small. The counter is therefore only defensive here, so this test
    /// covers the churn instead: the tree keeps its structure and contents, and no page ends up stuck
    /// owing levels it never pays back.
    /// </summary>
    [RavenFact(RavenTestCategory.Voron)]
    public void DeletingAndAddingBackWholeRangesKeepsTheTreeConsistent()
    {
        Slice.From(Allocator, "entries", out Slice treeName);
        var model = new SortedSet<long>();

        long next = Stride;
        while (true)
        {
            Add(treeName, model, ref next, 20_000);

            using (var tx = Env.ReadTransaction())
            {
                if (tx.FixedTreeFor(treeName, valSize: 8).Depth >= 3)
                    break;
            }
        }

        long last = next - Stride;
        for (int round = 0; round < 3; round++)
        {
            long from = last - 60_000 * Stride, to = last - round * 20_000 * Stride;

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeName, valSize: 8);
                foreach (long key in model.GetViewBetween(from, to))
                    fst.Delete(key);
                tx.Commit();
            }

            model.RemoveWhere(key => key >= from && key <= to);
            AssertConsistent(treeName, model);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeName, valSize: 8);
                for (long key = from; key <= to; key += Stride)
                {
                    if (model.Add(key))
                        fst.Add(key, Value);
                }

                tx.Commit();
            }

            AssertConsistent(treeName, model);
        }
    }

    private void AssertConsistent(Slice treeName, SortedSet<long> model)
    {
        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            fst.ValidateTree_Forced();
            Assert.All(AllPageHeaders(fst), h => Assert.True(h.CollapsedLevels <= PageCollapsedLevels.Max,
                $"a page is owing {h.CollapsedLevels} levels, more than the cap"));
        }

        AssertContents(treeName, model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MarkedPageMustWrapInPlaceWhenItSplits()
    {
        Slice.From(Allocator, "entries", out Slice treeName);
        var model = new SortedSet<long>();

        long next = Stride;
        while (true)
        {
            Add(treeName, model, ref next, 5_000);

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeName, valSize: 8);
                if (fst.Depth == 2 && ChildrenOf(fst, RootPage(fst)).Length >= 3)
                    break;
            }
        }

        long rootPage, markedPage, rangeStart, rangeEnd;
        int rootEntries;
        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            rootPage = RootPage(fst);
            var root = fst.GetReadOnlyPage(rootPage);
            rootEntries = root.NumberOfEntries;
            markedPage = root.GetEntry(1)->PageNumber;
            rangeStart = root.GetEntry(1)->GetKey<long>();
            rangeEnd = root.GetEntry(2)->GetKey<long>();
            Assert.True(fst.GetReadOnlyPage(markedPage).IsLeaf);
        }

        // a page that a collapse promoted is one level shallower than its siblings
        SetCollapsedLevels(treeName, markedPage, 1);

        // filling that range splits the marked leaf, which must wrap it in a branch in place rather
        // than add another leaf pointer to the root next to its branch children
        FillRange(treeName, model, rangeStart, rangeEnd);

        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            fst.ValidateTree_Forced();

            Assert.Equal(rootPage, RootPage(fst));
            var root = fst.GetReadOnlyPage(rootPage);

            // the split did not add a pointer to the root, it went under a branch that took the slot
            Assert.Equal(rootEntries, root.NumberOfEntries);

            long wrapperPage = root.GetEntry(1)->PageNumber;
            Assert.NotEqual(markedPage, wrapperPage);

            var wrapper = fst.GetReadOnlyPage(wrapperPage);
            Assert.True(wrapper.IsBranch);
            Assert.Equal(0, wrapper.CollapsedLevels);

            // the page that was marked is now a leaf under the wrapper, next to its split sibling
            Assert.Contains(markedPage, ChildrenOf(fst, wrapperPage));
            Assert.All(ChildrenOf(fst, wrapperPage), child => Assert.True(fst.GetReadOnlyPage(child).IsLeaf));
            Assert.Equal(0, fst.GetReadOnlyPage(markedPage).CollapsedLevels);
        }

        AssertContents(treeName, model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WrappingAPageThatOwesTwoLevelsMustLeaveItOwingOne()
    {
        Slice.From(Allocator, "entries", out Slice treeName);
        var model = new SortedSet<long>();

        long next = Stride;
        while (true)
        {
            Add(treeName, model, ref next, 5_000);

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeName, valSize: 8);
                if (fst.Depth == 2 && ChildrenOf(fst, RootPage(fst)).Length >= 3)
                    break;
            }
        }

        long rootPage, markedPage, rangeStart, rangeEnd;
        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            rootPage = RootPage(fst);
            var root = fst.GetReadOnlyPage(rootPage);
            markedPage = root.GetEntry(1)->PageNumber;
            rangeStart = root.GetEntry(1)->GetKey<long>();
            rangeEnd = root.GetEntry(2)->GetKey<long>();
        }

        // a page that collapsed twice between its splits is two levels shallower than its siblings
        SetCollapsedLevels(treeName, markedPage, 2);

        FillRange(treeName, model, rangeStart, rangeEnd);

        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            fst.ValidateTree_Forced();

            // a wrap restores a single level, so the wrapper is left owing the rest and wraps again on
            // its next split, while the page it wrapped sits next to its split sibling and owes nothing
            long wrapperPage = fst.GetReadOnlyPage(rootPage).GetEntry(1)->PageNumber;
            var wrapper = fst.GetReadOnlyPage(wrapperPage);
            Assert.True(wrapper.IsBranch);
            Assert.Equal(1, wrapper.CollapsedLevels);
            Assert.All(ChildrenOf(fst, wrapperPage), child => Assert.Equal(0, fst.GetReadOnlyPage(child).CollapsedLevels));
        }

        AssertContents(treeName, model);
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(true)]
    [InlineData(false)]
    public void BranchesThatOweDifferentLevelsMustNotMerge(bool markedPageShrinks)
    {
        Slice.From(Allocator, "entries", out Slice treeName);
        var model = new SortedSet<long>();

        long next = Stride;
        while (true)
        {
            Add(treeName, model, ref next, 20_000);

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeName, valSize: 8);
                if (fst.Depth == 3 && ChildrenOf(fst, RootPage(fst)).Length >= 3)
                    break;
            }
        }

        long rootPage, unmarkedPage, markedPage;
        int rootEntries;
        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            rootPage = RootPage(fst);
            var children = ChildrenOf(fst, rootPage);
            rootEntries = children.Length;
            unmarkedPage = children[0];
            markedPage = children[1];
        }

        long triggerPage = markedPageShrinks ? markedPage : unmarkedPage;
        long otherPage = markedPageShrinks ? unmarkedPage : markedPage;
        int halfAPage = Constants.Storage.PageSize / FixedSizeTree.BranchEntrySize / 2;
        int quarterAPage = halfAPage / 2;
        RemoveLastLeavesOf(treeName, model, otherPage, fst => fst.GetReadOnlyPage(otherPage).NumberOfEntries <= halfAPage);

        SetCollapsedLevels(treeName, markedPage, 1);

        RemoveLastLeavesOf(treeName, model, triggerPage, fst => fst.GetReadOnlyPage(rootPage).NumberOfEntries < rootEntries || fst.GetReadOnlyPage(triggerPage).NumberOfEntries <= quarterAPage);

        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            fst.ValidateTree_Forced();

            var children = ChildrenOf(fst, rootPage);
            Assert.Contains(markedPage, children);
            Assert.Contains(unmarkedPage, children);
            Assert.Equal(1, fst.GetReadOnlyPage(markedPage).CollapsedLevels);
            Assert.Equal(0, fst.GetReadOnlyPage(unmarkedPage).CollapsedLevels);
        }

        AssertContents(treeName, model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PageThatBecomesTheRootMustNotOweLevels()
    {
        Slice.From(Allocator, "entries", out Slice treeName);

        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            long next = Stride;
            while (fst.Depth < 3)
            {
                fst.Add(next, Value);
                next += Stride;
            }

            fst.Delete(next - Stride);

            var rootChildren = ChildrenOf(fst, RootPage(fst));
            Assert.Equal(2, rootChildren.Length);
            Assert.True(fst.GetReadOnlyPage(rootChildren[0]).IsBranch);
            var owingLeaf = fst.GetReadOnlyPage(rootChildren[1]);
            Assert.Equal(1, owingLeaf.CollapsedLevels);

            // deleting one key at a time lets the rebalancer move entries of the owing leaf to the left, so delete until the root
            // collapses. Depth is not a stop condition: a branch collapsing into its parent does not decrement it
            int depth = fst.Depth;
            for (long key = Stride; key < next; key += Stride)
            {
                fst.Delete(key);
                if (fst.Depth == depth)
                    continue;

                depth = fst.Depth;
                if (fst.GetReadOnlyPage(RootPage(fst)).IsLeaf)
                    break;
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            var root = fst.GetReadOnlyPage(RootPage(fst));
            Assert.True(root.IsLeaf);
            Assert.True(root.CollapsedLevels == 0, $"the root is owing {root.CollapsedLevels} levels, but it has no siblings to be shallower than");
        }
    }

    private void RemoveLastLeavesOf(Slice treeName, SortedSet<long> model, long branchPage, Func<FixedSizeTree, bool> until)
    {
        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            while (until(fst) == false)
            {
                var branch = fst.GetReadOnlyPage(branchPage);
                var leaf = fst.GetReadOnlyPage(branch.GetEntry(branch.NumberOfEntries - 1)->PageNumber);
                long from = leaf.GetKey(0), to = leaf.GetKey(leaf.NumberOfEntries - 1);
                var keys = model.GetViewBetween(from, to);
                Assert.True(keys.Count > 0, $"the last leaf of branch {branchPage} holds no live key, the loop would never end");
                foreach (long key in keys)
                    fst.Delete(key);
                keys.Clear();
            }

            tx.Commit();
        }
    }

    private void Add(Slice treeName, SortedSet<long> model, ref long next, int count)
    {
        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            for (int i = 0; i < count; i++, next += Stride)
            {
                fst.Add(next, Value);
                model.Add(next);
            }

            tx.Commit();
        }
    }

    /// <summary>
    /// Doubles the density of the given range, which overflows the leaf covering it and forces a split.
    /// </summary>
    private void FillRange(Slice treeName, SortedSet<long> model, long from, long toExclusive)
    {
        using (var tx = Env.WriteTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            for (long key = from + Stride / 2; key < toExclusive; key += Stride)
            {
                if (model.Add(key))
                    fst.Add(key, Value);
            }

            tx.Commit();
        }
    }

    private void SetCollapsedLevels(Slice treeName, long pageNumber, byte levels)
    {
        using (var tx = Env.WriteTransaction())
        {
            // make sure the tree is opened, so the page belongs to it
            tx.FixedTreeFor(treeName, valSize: 8);
            var page = tx.LowLevelTransaction.ModifyPage(pageNumber);
            ((FixedSizeTreePageHeader*)page.Pointer)->CollapsedLevels = levels;
            tx.Commit();
        }
    }

    private void AssertContents(Slice treeName, SortedSet<long> model)
    {
        using (var tx = Env.ReadTransaction())
        {
            var fst = tx.FixedTreeFor(treeName, valSize: 8);
            Assert.Equal(model.Count, fst.NumberOfEntries);

            var actual = new List<long>();
            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue))
                {
                    do
                    {
                        actual.Add(it.CurrentKey);
                    } while (it.MoveNext());
                }
            }

            Assert.Equal(model.ToList(), actual);
        }
    }

    private static long RootPage(FixedSizeTree fst)
    {
        var candidates = fst.AllPages().ToHashSet();
        foreach (long pageNumber in fst.AllPages())
        {
            var page = fst.GetReadOnlyPage(pageNumber);
            if (page.IsBranch == false)
                continue;

            for (int i = 0; i < page.NumberOfEntries; i++)
                candidates.Remove(page.GetEntry(i)->PageNumber);
        }

        return Assert.Single(candidates);
    }

    private static long[] ChildrenOf(FixedSizeTree fst, long pageNumber)
    {
        var page = fst.GetReadOnlyPage(pageNumber);
        if (page.IsBranch == false)
            return [];

        var children = new long[page.NumberOfEntries];
        for (int i = 0; i < children.Length; i++)
            children[i] = page.GetEntry(i)->PageNumber;
        return children;
    }

    private static List<FixedSizeTreePageHeader> AllPageHeaders(FixedSizeTree fst)
    {
        return fst.AllPages().Select(fst.GetPageHeader).ToList();
    }
}
