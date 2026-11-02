using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.PostingLists;
using Xunit;

namespace SlowTests.Issues;

public unsafe class RavenDB_27533_PostingList(ITestOutputHelper output) : StorageTest(output)
{
    private const string Name = "entries";

    [RavenFact(RavenTestCategory.Voron)]
    public void BothPostingListPageHeadersKeepTheCounterAtTheSameOffset()
    {
        var leaf = stackalloc PostingListLeafPageHeader[1];
        var branch = (PostingListBranchPageHeader*)leaf;

        Unsafe.InitBlock(leaf, 0, (uint)sizeof(PostingListLeafPageHeader));
        leaf->CollapsedLevels = 3;

        // the collapse path increments the counter without knowing the page type, so the two headers
        // must agree on where it lives
        Assert.Equal(3, branch->CollapsedLevels);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CollapsedPageMustWrapInPlaceInsteadOfAddingALeafNextToBranches()
    {
        var model = new SortedSet<long>();
        long step = GrowUntilRootHasLeaves(model, atLeast: 3);

        var children = RootChildren();
        long markedPage = children[1].Page;
        Assert.Equal(ExtendedPageType.PostingListLeaf, TypeOf(markedPage));

        // a leaf that a collapse promoted is one level shallower than its branch siblings
        SetCollapsedLevels(markedPage, 1);

        // overflowing it must wrap it in a branch in place, reusing the page number the root points to,
        // instead of adding another leaf pointer next to the root's other children
        FillRange(model, children[1].Key, children[2].Key, step);

        Assert.Equal(children.Length, RootChildren().Length);
        Assert.Equal(markedPage, RootChildren()[1].Page);
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(markedPage));
        Assert.Equal(0, CollapsedLevelsOf(markedPage));
        AssertChildrenAreLeaves(markedPage);

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WrappingAPageThatOwesTwoLevelsMustLeaveItOwingOne()
    {
        var model = new SortedSet<long>();
        long step = GrowUntilRootHasLeaves(model, atLeast: 3);

        var children = RootChildren();
        long markedPage = children[1].Page;

        // a page that collapsed twice between its splits is two levels shallower than its siblings
        SetCollapsedLevels(markedPage, 2);

        // a wrap restores a single level, so the wrapper is left owing the rest and will wrap again
        // the next time it splits
        FillRange(model, children[1].Key, children[2].Key, step);

        Assert.Equal(children.Length, RootChildren().Length);
        Assert.Equal(markedPage, RootChildren()[1].Page);
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(markedPage));
        Assert.Equal(1, CollapsedLevelsOf(markedPage));
        AssertChildrenAreLeaves(markedPage);

        // the copy that moved under the wrapper sits next to its split sibling, it owes nothing
        Assert.All(ChildrenOf(markedPage), child => Assert.Equal(0, CollapsedLevelsOf(child)));

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void UnmarkedPageFromAnOlderVersionNextToABranchSiblingIsStillWrapped()
    {
        var model = new SortedSet<long>();
        long step = GrowUntilRootHasLeaves(model, atLeast: 4);

        var children = RootChildren();

        // turn the second child into a branch, so the third child has a branch sibling
        SetCollapsedLevels(children[1].Page, 1);
        FillRange(model, children[1].Key, children[2].Key, step);
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(children[1].Page));

        // pages written before the counter existed aren't marked, the sibling probes must still
        // detect the branch sibling and wrap rather than mixing leaf and branch pointers
        Assert.Equal(0, CollapsedLevelsOf(children[2].Page));
        Assert.Equal(ExtendedPageType.PostingListLeaf, TypeOf(children[2].Page));

        FillRange(model, children[2].Key, children[3].Key, step);

        Assert.Equal(children.Length, RootChildren().Length);
        Assert.Equal(children[2].Page, RootChildren()[2].Page);
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(children[2].Page));

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CollapsingABranchBelowTheRootMustMarkTheSurvivingChild()
    {
        const long step = 1L << 20;
        var model = new SortedSet<long>();
        long next = step;

        // grow until the tree is deep enough to have branches that are not the root
        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 100_000; i++)
                {
                    list.Add(next);
                    model.Add(next);
                    next += step;
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                if (rtx.OpenPostingList(Name).State.Depth >= 3)
                    break;
            }
        }

        // drain the tree from the top down, this empties leaves under one branch at a time, which
        // merges them and eventually collapses their parent into the last surviving child
        bool sawMarkedPage = false;
        while (model.Count > 0 && sawMarkedPage == false)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                foreach (long value in model.Reverse().Take(20_000).ToList())
                {
                    list.Remove(value);
                    model.Remove(value);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                list.Verify();
                sawMarkedPage = AllPagesOf(list).Any(page => CollapsedLevelsOf(list, page) > 0);
            }
        }

        Assert.True(sawMarkedPage, "a collapse below the root should have marked the surviving child");
        AssertContents(model);

        // adding back into the marked page must wrap it rather than mix leaf and branch pointers
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            for (int i = 0; i < 200_000; i++)
            {
                list.Add(next);
                model.Add(next);
                next += step;
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            list.Verify();
        }

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BranchesThatOweDifferentLevelsMustNotMerge()
    {
        const long step = 1L << 20;
        var model = new SortedSet<long>();
        long next = step;

        (long Key, long Page)[] rootChildren;
        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 100_000; i++)
                {
                    list.Add(next);
                    model.Add(next);
                    next += step;
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                if (list.State.Depth < 3)
                    continue;

                rootChildren = RootChildren(list);
                if (rootChildren.Length >= 3)
                    break;
            }
        }

        long siblingPage = rootChildren[0].Page;
        long currentPage = rootChildren[1].Page;
        Assert.True(BranchEntryCount(siblingPage) > PostingListBranchPage.MinNumberOfValuesBeforeMerge);
        Assert.True(BranchEntryCount(currentPage) > PostingListBranchPage.MinNumberOfValuesBeforeMerge);

        while (BranchEntryCount(siblingPage) > PostingListBranchPage.MinNumberOfValuesBeforeMerge)
            RemoveLastLeafFromBranch(siblingPage, model);

        Assert.Contains(RootChildren(), child => child.Page == siblingPage);
        SetCollapsedLevels(siblingPage, 1);

        int numberOfRootChildren = RootChildren().Length;
        while (RootChildren().Length == numberOfRootChildren && BranchEntryCount(currentPage) > PostingListBranchPage.MinNumberOfValuesBeforeMerge)
            RemoveLastLeafFromBranch(currentPage, model);

        Assert.Contains(RootChildren(), child => child.Page == siblingPage);
        Assert.Contains(RootChildren(), child => child.Page == currentPage);
        Assert.Equal(1, CollapsedLevelsOf(siblingPage));
        Assert.Equal(0, CollapsedLevelsOf(currentPage));
        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CollapsingABranchThatOwesALevelMustPassItToItsChild()
    {
        var model = new SortedSet<long>();
        GrowUntilDepthThree(model, atLeastRootChildren: 3);

        var rootChildren = RootChildren();
        long markedBranch = rootChildren[^1].Page;
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(markedBranch));

        // pretend an earlier collapse left this branch one level shallower than its siblings
        SetCollapsedLevels(markedBranch, 1);

        // drain from the top: the marked branch cannot merge into its sibling of a different height, so it
        // shrinks until its last two leaves merge and it collapses into the survivor
        for (int round = 0; TypeOf(markedBranch) == ExtendedPageType.PostingListBranch; round++)
        {
            Assert.True(round < 200 && model.Count > 0, "the marked branch never collapsed");
            RemoveFromTop(model, 20_000);
        }

        Assert.Equal(rootChildren.Length, RootChildren().Length);
        Assert.Equal(markedBranch, RootChildren()[^1].Page);
        Assert.Equal(ExtendedPageType.PostingListLeaf, TypeOf(markedBranch));

        // the branch owed one level and its child was collapsed into it, so the survivor owes both
        Assert.Equal(2, CollapsedLevelsOf(markedBranch));

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MarkedBranchThatOverflowsMustWrapBeforeItSplits()
    {
        var model = new SortedSet<long>();
        long gap = GrowUntilDepthThree(model, atLeastRootChildren: 3);

        var rootChildren = RootChildren();
        long markedBranch = rootChildren[0].Page;
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(markedBranch));
        Assert.All(ChildrenOf(markedBranch), child => Assert.Equal(ExtendedPageType.PostingListLeaf, TypeOf(child)));

        // pretend a collapse left this whole subtree one level shallower than its siblings
        SetCollapsedLevels(markedBranch, 1);

        // doubling the density of its last leaves splits them, until the branch has no room for another leaf
        // and has to split itself: that must wrap it in place, not split it next to its siblings in the root
        long from = ChildrenWithKeys(markedBranch)[^8].Key;
        while (TypeOf(ChildrenOf(markedBranch)[0]) == ExtendedPageType.PostingListLeaf && RootChildren().Length == rootChildren.Length)
        {
            FillRange(model, from, rootChildren[1].Key, gap);
            gap /= 2;
        }

        Assert.Equal(rootChildren.Length, RootChildren().Length);
        Assert.Equal(markedBranch, RootChildren()[0].Page);
        Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(markedBranch));
        Assert.Equal(0, CollapsedLevelsOf(markedBranch));

        var wrapped = ChildrenOf(markedBranch);
        Assert.True(wrapped.Length >= 2, "the wrapper should hold the branch it wrapped and its split sibling");
        Assert.All(wrapped, child =>
        {
            Assert.Equal(ExtendedPageType.PostingListBranch, TypeOf(child));
            Assert.Equal(0, CollapsedLevelsOf(child));
            AssertChildrenAreLeaves(child);
        });

        AssertContents(model);
        AssertEveryPageHoldsOnlyItsRange();
    }

    /// <summary>
    /// Adds ascending values until the tree has three levels and the root at least the requested number of children.
    /// Wide gaps keep the encoded values big, so a leaf holds few of them and the tree gets deep with fewer values.
    /// Returns the gap between consecutive values.
    /// </summary>
    private long GrowUntilDepthThree(SortedSet<long> model, int atLeastRootChildren)
    {
        const long step = 1L << 40;
        long next = step;

        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 100_000; i++, next += step)
                {
                    list.Add(next);
                    model.Add(next);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                if (list.State.Depth >= 3 && RootChildren(list).Length >= atLeastRootChildren)
                    return step;
            }
        }
    }

    private void RemoveFromTop(SortedSet<long> model, int count)
    {
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            foreach (long value in model.Reverse().Take(count).ToList())
            {
                list.Remove(value);
                model.Remove(value);
            }

            wtx.Commit();
        }
    }

    private (long Key, long Page)[] ChildrenWithKeys(long branchPageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var branch = new PostingListBranchPage(list.Llt.GetPage(branchPageNumber));
            var children = new (long, long)[branch.Header->NumberOfEntries];
            for (int i = 0; i < children.Length; i++)
                children[i] = branch.GetByIndex(i);
            return children;
        }
    }

    /// <summary>
    /// Every page must only hold values inside the range its parent assigns to it. A wrapper that took over the range
    /// of the page it wrapped with a wrong separator shows up here, and as values that route to a page which does not
    /// hold them. PostingList.Verify does the same, but it is compiled out of Release builds.
    /// </summary>
    private void AssertEveryPageHoldsOnlyItsRange()
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            AssertRange(list, list.State.RootPage, long.MinValue, long.MaxValue);
        }
    }

    private static void AssertRange(PostingList list, long pageNumber, long min, long maxExclusive)
    {
        var page = list.Llt.GetPage(pageNumber);
        if (((PostingListLeafPageHeader*)page.Pointer)->PageType == ExtendedPageType.PostingListLeaf)
        {
            var values = new PostingListLeafPage(page).GetDebugOutput();
            Assert.All(values, value => Assert.True(value >= min && value < maxExclusive, $"page {pageNumber} holds {value}, outside of its range [{min}, {maxExclusive})"));
            return;
        }

        var branch = new PostingListBranchPage(page);
        for (int i = 0; i < branch.Header->NumberOfEntries; i++)
        {
            (long key, long child) = branch.GetByIndex(i);
            Assert.True(key >= min, $"page {pageNumber} routes {key} to page {child}, below its own range start {min}");
            long end = i + 1 < branch.Header->NumberOfEntries ? branch.GetByIndex(i + 1).Item1 : maxExclusive;
            AssertRange(list, child, key, end);
        }
    }

    private static List<long> AllPagesOf(PostingList list)
    {
        return list.AllPages();
    }

    private static int CollapsedLevelsOf(PostingList list, long pageNumber)
    {
        return ((PostingListLeafPageHeader*)list.Llt.GetPage(pageNumber).Pointer)->CollapsedLevels;
    }

    private int BranchEntryCount(long pageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            return new PostingListBranchPage(list.Llt.GetPage(pageNumber)).Header->NumberOfEntries;
        }
    }

    private void RemoveLastLeafFromBranch(long branchPageNumber, SortedSet<long> model)
    {
        List<long> values;
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var branch = new PostingListBranchPage(list.Llt.GetPage(branchPageNumber));
            (_, long leafPageNumber) = branch.GetByIndex(branch.Header->NumberOfEntries - 1);
            values = new PostingListLeafPage(list.Llt.GetPage(leafPageNumber)).GetDebugOutput();
        }

        Assert.NotEmpty(values);
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            foreach (long value in values)
            {
                list.Remove(value);
                model.Remove(value);
            }

            wtx.Commit();
        }
    }

    /// <summary>
    /// Adds ascending values until the root is a branch with at least the requested number of leaves,
    /// returns the gap left between consecutive values, so callers can fill a range later.
    /// </summary>
    private long GrowUntilRootHasLeaves(SortedSet<long> model, int atLeast)
    {
        const long step = 1L << 20;
        long next = step;

        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 4096; i++)
                {
                    list.Add(next);
                    model.Add(next);
                    next += step;
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                if (list.State.Depth == 2 && RootChildren(list).Length >= atLeast)
                    return step;
            }
        }
    }

    /// <summary>
    /// Doubles the density of the given range by adding a value in the middle of every existing gap,
    /// which overflows the leaf that covers the range and forces it to split.
    /// </summary>
    private void FillRange(SortedSet<long> model, long from, long toExclusive, long gap)
    {
        if (gap < 4)
            throw new InvalidOperationException("Ran out of room to add values between the existing ones");

        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            for (long value = from + gap / 2; value < toExclusive; value += gap)
            {
                long even = value & ~1L; // posting lists only accept even values
                if (even > from && model.Add(even))
                    list.Add(even);
            }

            wtx.Commit();
        }
    }

    private void SetCollapsedLevels(long pageNumber, byte levels)
    {
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            var page = list.Llt.ModifyPage(pageNumber);
            ((PostingListLeafPageHeader*)page.Pointer)->CollapsedLevels = levels;
            wtx.Commit();
        }
    }

    private void AssertChildrenAreLeaves(long pageNumber)
    {
        var children = ChildrenOf(pageNumber);
        Assert.NotEmpty(children);
        Assert.All(children, child => Assert.Equal(ExtendedPageType.PostingListLeaf, TypeOf(child)));
    }

    private void AssertContents(SortedSet<long> model)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            Assert.Equal(model.Count, list.State.NumberOfEntries);
            Assert.Equal(model.ToList(), AllValues(list));
        }
    }

    private (long Key, long Page)[] RootChildren()
    {
        using (var rtx = Env.ReadTransaction())
            return RootChildren(rtx.OpenPostingList(Name));
    }

    private static (long Key, long Page)[] RootChildren(PostingList list)
    {
        var branch = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
        var children = new (long, long)[branch.Header->NumberOfEntries];
        for (int i = 0; i < children.Length; i++)
            children[i] = branch.GetByIndex(i);
        return children;
    }

    private long[] ChildrenOf(long pageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            return new PostingListBranchPage(list.Llt.GetPage(pageNumber)).GetAllChildPages().ToArray();
        }
    }

    private ExtendedPageType TypeOf(long pageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            return ((PostingListLeafPageHeader*)list.Llt.GetPage(pageNumber).Pointer)->PageType;
        }
    }

    private int CollapsedLevelsOf(long pageNumber)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            return ((PostingListLeafPageHeader*)list.Llt.GetPage(pageNumber).Pointer)->CollapsedLevels;
        }
    }

    private static int[] LeafDepths(PostingList list, long pageNumber, int depth)
    {
        var header = (PostingListLeafPageHeader*)list.Llt.GetPage(pageNumber).Pointer;
        if (header->PageType == ExtendedPageType.PostingListLeaf)
            return new[] { depth };

        return new PostingListBranchPage(list.Llt.GetPage(pageNumber))
            .GetAllChildPages()
            .SelectMany(child => LeafDepths(list, child, depth + 1))
            .Distinct()
            .ToArray();
    }

    private static List<long> AllValues(PostingList list)
    {
        var it = list.Iterate();
        var result = new List<long>();
        Span<long> buffer = stackalloc long[1024];
        if (it.Seek(0) == false)
            return result;
        while (it.Fill(buffer, out int read) && read != 0)
        {
            for (int i = 0; i < read; i++)
                result.Add(buffer[i]);
        }

        return result;
    }
}
