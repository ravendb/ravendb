using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

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
    public void MergingBranchesMustKeepTheCollapsedLevelsOfTheSibling()
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
        while (RootChildren().Length == numberOfRootChildren)
            RemoveLastLeafFromBranch(currentPage, model);

        Assert.DoesNotContain(RootChildren(), child => child.Page == siblingPage);
        Assert.Contains(RootChildren(), child => child.Page == currentPage);
        Assert.Equal(1, CollapsedLevelsOf(currentPage));
        AssertContents(model);
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
