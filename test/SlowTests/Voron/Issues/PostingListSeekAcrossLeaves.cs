using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.PostingLists;
using Xunit;

namespace SlowTests.Voron.Issues;

/// <summary>
/// PostingList.Iterator.Seek promises "false: no element equal to or greater than the given parameter exists", and
/// TermMatch.AndWithFunc returns no matches at all when it gets false. Seek only looks at the leaf the value routes
/// to, so it answers false whenever that leaf has nothing at or above the value, even if the next leaf does.
/// </summary>
public unsafe class PostingListSeekAcrossLeaves(ITestOutputHelper output) : StorageTest(output)
{
    private const string Name = "entries";

    [RavenFact(RavenTestCategory.Voron)]
    public void SeekIntoTheGapAtTheEndOfALeafMustFindTheFirstValueOfTheNextLeaf()
    {
        const long step = 4;
        var model = new SortedSet<long>();
        long next = step;

        while (true)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                for (int i = 0; i < 4096; i++, next += step)
                {
                    list.Add(next);
                    model.Add(next);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                if (list.State.Depth == 2 && new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage)).Header->NumberOfEntries >= 2)
                    break;
            }
        }

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
            (long firstOfSecondLeaf, long secondLeaf) = root.GetByIndex(1);
            Assert.Contains(firstOfSecondLeaf, model);
            Assert.Equal(firstOfSecondLeaf, new PostingListLeafPage(list.Llt.GetPage(secondLeaf)).GetDebugOutput()[0]);

            long from = firstOfSecondLeaf - 2; // above everything in the first leaf, below everything in the second
            Assert.DoesNotContain(from, model);

            var it = list.Iterate();
            Assert.True(it.Seek(from), $"Seek({from}) said nothing >= {from} exists, but {firstOfSecondLeaf} does");

            Span<long> buffer = stackalloc long[16];
            Assert.True(it.Fill(buffer, out int read) && read > 0);
            Assert.Equal(firstOfSecondLeaf, buffer[0]);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SeekIntoAnEmptiedCollapsedLeafMustFindTheValuesOfTheNextLeaf()
    {
        const long step = 1L << 20;
        var model = new SortedSet<long>();
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
                // three children under the root, so the drained branch cannot merge into its sibling before it collapses
                var list = rtx.OpenPostingList(Name);
                if (list.State.Depth >= 3 && new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage)).Header->NumberOfEntries >= 3)
                    break;
            }
        }

        // drain from the bottom until a branch below the root collapses into its last leaf, so values remain above it
        long markedLeaf = -1, lo = 0, hi = 0;
        while (model.Count > 0 && markedLeaf == -1)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                foreach (long value in model.Take(5_000).ToList())
                {
                    list.Remove(value);
                    model.Remove(value);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                for (int i = 0; i < root.Header->NumberOfEntries; i++)
                {
                    (long key, long pageNumber) = root.GetByIndex(i);
                    var header = (PostingListLeafPageHeader*)list.Llt.GetPage(pageNumber).Pointer;
                    if (header->PageType == ExtendedPageType.PostingListLeaf && header->CollapsedLevels > 0)
                    {
                        markedLeaf = pageNumber;
                        lo = key;
                        hi = i + 1 < root.Header->NumberOfEntries ? root.GetByIndex(i + 1).Item1 : long.MaxValue;
                        break;
                    }
                }
            }
        }

        Assert.NotEqual(-1, markedLeaf);

        // its siblings under the root are branches, so once it is emptied nothing merges it away
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            foreach (long value in model.GetViewBetween(lo, hi - 1).ToList())
            {
                list.Remove(value);
                model.Remove(value);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var header = (PostingListLeafPageHeader*)list.Llt.GetPage(markedLeaf).Pointer;
            Assert.Equal(ExtendedPageType.PostingListLeaf, header->PageType);
            Assert.Equal(0, header->NumberOfEntries);

            long from = (lo == long.MinValue ? 0 : lo) + 2; // inside the empty leaf's range, not a stored value
            Assert.DoesNotContain(from, model);
            var above = model.GetViewBetween(from, long.MaxValue);
            Assert.True(above.Count > 0, "values above the emptied leaf are needed for the seek to have something to find");

            var it = list.Iterate();
            Assert.True(it.Seek(from), $"Seek({from}) said nothing >= {from} exists, but {above.Min} does");

            Span<long> buffer = stackalloc long[16];
            Assert.True(it.Fill(buffer, out int read) && read > 0);
            Assert.Equal(above.Min, buffer[0]);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SeekIntoConsecutiveEmptiedLeavesMustFindTheValuesOfTheNextLeaf()
    {
        var model = new SortedSet<long>();
        var (k1, k2, _) = EmptyTwoConsecutiveRootChildren(model);

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);

            // both seeks land in an emptied leaf, and the next leaf with values is behind another emptied leaf
            foreach (long from in new[] { k1 + 2, k2 + 2 })
            {
                Assert.DoesNotContain(from, model);
                var above = model.GetViewBetween(from, long.MaxValue);

                var it = list.Iterate();
                Assert.True(it.Seek(from), $"Seek({from}) said nothing >= {from} exists, but {above.Min} does");

                Span<long> buffer = stackalloc long[16];
                Assert.True(it.Fill(buffer, out int read) && read > 0);
                Assert.Equal(above.Min, buffer[0]);
            }

            var beyond = list.Iterate();
            Assert.False(beyond.Seek(model.Max + 2), "nothing is >= a value above the maximum");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void FillAcrossConsecutiveEmptiedLeavesMustNotStop()
    {
        var model = new SortedSet<long>();
        var (k1, _, _) = EmptyTwoConsecutiveRootChildren(model);

        long lastBeforeTheGap = model.GetViewBetween(long.MinValue, k1 - 1).Max;
        var expected = model.GetViewBetween(lastBeforeTheGap, long.MaxValue).Take(5_000).ToList();

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var it = list.Iterate();
            Assert.True(it.Seek(lastBeforeTheGap));

            // a small buffer, so that filling has to walk past the emptied leaves on its own. Seek only finds the
            // leaf, so the first values may be older than the one it was given
            var actual = new List<long>();
            Span<long> buffer = stackalloc long[64];
            while (actual.Count < expected.Count && it.Fill(buffer, out int read) && read > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    if (buffer[i] >= lastBeforeTheGap)
                        actual.Add(buffer[i]);
                }
            }

            Assert.Equal(expected, actual.Take(expected.Count).ToList());
        }
    }

    /// <summary>
    /// Builds a tree whose root has at least four children, then drains the second and the third of them: each
    /// collapses into a leaf as it drains and is emptied, and the leaf stays because its left sibling is a branch
    /// and nothing merges a leaf into a branch. Returns the first value of the range of the children 1, 2 and 3.
    /// </summary>
    private (long K1, long K2, long K3) EmptyTwoConsecutiveRootChildren(SortedSet<long> model)
    {
        const long step = 1L << 40;
        long next = step;

        (long Key, long Page)[] children;
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
                if (list.State.Depth < 3)
                    continue;

                children = RootChildren(list);
                if (children.Length >= 4)
                    break;
            }
        }

        // the later one first, so that the one in front of it is still a branch when it drains
        Drain(model, children[2].Key, children[3].Key - 1);
        Drain(model, children[1].Key, children[2].Key - 1);

        using (var rtx = Env.ReadTransaction())
        {
            var list = rtx.OpenPostingList(Name);
            var now = RootChildren(list);
            Assert.Equal(children.Length, now.Length);

            Assert.Equal(ExtendedPageType.PostingListBranch, ((PostingListLeafPageHeader*)list.Llt.GetPage(now[0].Page).Pointer)->PageType);
            Assert.Equal(ExtendedPageType.PostingListBranch, ((PostingListLeafPageHeader*)list.Llt.GetPage(now[3].Page).Pointer)->PageType);
            foreach (int emptied in new[] { 1, 2 })
            {
                var header = (PostingListLeafPageHeader*)list.Llt.GetPage(now[emptied].Page).Pointer;
                Assert.Equal(ExtendedPageType.PostingListLeaf, header->PageType);
                Assert.Equal(0, header->NumberOfEntries);
            }
        }

        return (children[1].Key, children[2].Key, children[3].Key);
    }

    private void Drain(SortedSet<long> model, long from, long toInclusive)
    {
        using (var wtx = Env.WriteTransaction())
        {
            var list = wtx.OpenPostingList(Name);
            foreach (long value in model.GetViewBetween(from, toInclusive).ToList())
            {
                list.Remove(value);
                model.Remove(value);
            }

            wtx.Commit();
        }
    }

    private static (long Key, long Page)[] RootChildren(PostingList list)
    {
        var branch = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
        var children = new (long, long)[branch.Header->NumberOfEntries];
        for (int i = 0; i < children.Length; i++)
            children[i] = branch.GetByIndex(i);
        return children;
    }
}
