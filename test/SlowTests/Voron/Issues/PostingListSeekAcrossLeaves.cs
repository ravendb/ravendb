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
}
