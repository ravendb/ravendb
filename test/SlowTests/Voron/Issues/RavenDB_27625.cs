using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Graphs;
using Voron.Data.PostingLists;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public unsafe class RavenDB_27625(ITestOutputHelper output) : StorageTest(output)
    {
        private const string Name = "entries";
        private const long Step = 1L << 34;

        [RavenFact(RavenTestCategory.Voron)]
        public void DrainingAPostingListToEmptyMustLeaveASinglePage()
        {
            var model = new SortedSet<long>();
            long lastRootChildKey = FillUntilThreeLevelsWithThreeRootChildren(model);

            // the last root child collapses into a leaf as it drains and is then emptied while its left sibling is a branch
            Remove(model.GetViewBetween(lastRootChildKey, long.MaxValue).ToList(), model);

            while (model.Count > 0)
                Remove(model.Reverse().Take(100_000).ToList(), model);

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                Assert.Equal(0, list.State.NumberOfEntries);

                var pages = list.AllPages();
                Assert.True(pages.Count == 1,
                    $"the emptied posting list still owns {pages.Count} pages ({string.Join(", ", pages)}): depth {list.State.Depth}, {list.State.BranchPages} branches, {list.State.LeafPages} leaves");
            }
        }

        public enum DrainOrder
        {
            NewestFirst,
            OldestFirst,
            Random
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(DrainOrder.NewestFirst)]
        [InlineData(DrainOrder.OldestFirst)]
        [InlineData(DrainOrder.Random)]
        public void DrainingNeverLeavesAnEmptyLeafBehind(DrainOrder order)
        {
            var model = new SortedSet<long>();
            long lastRootChildKey = FillUntilThreeLevelsWithThreeRootChildren(model);

            if (order == DrainOrder.NewestFirst)
            {
                Remove(model.GetViewBetween(lastRootChildKey, long.MaxValue).ToList(), model);
                AssertShape(model);
            }

            var random = new Random(27625);
            List<long> pending = order switch
            {
                DrainOrder.NewestFirst => model.Reverse().ToList(),
                DrainOrder.OldestFirst => model.ToList(),
                _ => model.OrderBy(_ => random.Next()).ToList()
            };

            for (int i = 0; i < pending.Count; i += 100_000)
            {
                Remove(pending.Skip(i).Take(100_000).ToList(), model);
                AssertShape(model);
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                Assert.Equal(0, list.State.NumberOfEntries);
                Assert.Single(list.AllPages());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SeekBelowTheRemainingValuesAfterDrainingOldestFirst()
        {
            var model = new SortedSet<long>();
            FillUntilThreeLevelsWithThreeRootChildren(model);

            var pending = model.ToList();
            for (int i = 0; i < pending.Count; i += 100_000)
            {
                Remove(pending.Skip(i).Take(100_000).ToList(), model);
                if (model.Count == 0)
                    break;

                using (var rtx = Env.ReadTransaction())
                {
                    var list = rtx.OpenPostingList(Name);
                    foreach (long from in new[] { long.MinValue, 0, Step, model.Min - 2 })
                    {
                        var it = list.Iterate();
                        Assert.True(it.Seek(from), $"Seek({from}) found nothing, {model.Count} remain from {model.Min}");
                        Span<long> buffer = stackalloc long[16];
                        Assert.True(it.Fill(buffer, out int read) && read > 0, $"Seek({from}) read nothing, {model.Count} remain from {model.Min}");
                        Assert.True(model.Min == buffer[0], $"Seek({from}) returned {buffer[0]}, expected {model.Min}, {model.Count} remain");
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AddingBelowTheFirstKeyAfterTheFirstChildIsUnlinked()
        {
            var model = new SortedSet<long>();
            FillUntilThreeLevelsWithThreeRootChildren(model);

            // drain oldest first until the first child of the root is unlinked, and its branch sibling becomes the first child
            var all = model.ToList();
            for (int i = 0; ; i += 100_000)
            {
                Assert.True(i < all.Count, "the first child of the root was never unlinked");
                Remove(all.Skip(i).Take(100_000).ToList(), model);

                using (var rtx = Env.ReadTransaction())
                {
                    var list = rtx.OpenPostingList(Name);
                    var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                    Assert.Equal(ExtendedPageType.PostingListBranch, root.Header->PageType);
                    var first = (PostingListLeafPageHeader*)list.Llt.GetPage(root.GetByIndex(0).Page).Pointer;
                    if (root.Header->NumberOfEntries < 3 && first->PageType == ExtendedPageType.PostingListBranch)
                        break;
                }
            }

            // the values below the key of the new first child split its first leaf, and their separators must not
            // sort in front of that leaf
            Add(all.Take(200_000).ToList(), model);

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                Assert.Equal(model, list.DumpAllValues());

                int lost = 0;
                long firstLost = -1;
                foreach (long value in model.Where((_, idx) => idx % 97 == 0))
                {
                    if (FirstAtOrAbove(list, value) == value)
                        continue;
                    if (lost++ == 0)
                        firstLost = value;
                }

                Assert.True(lost == 0, $"Seek missed {lost} sampled values, first {firstLost}");
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(false)]
        [InlineData(true)]
        public void MergingBranchesWhoseFirstKeyIsNotTheirLowerBound(bool drainTheRightBranchFirst)
        {
            var model = new SortedSet<long>();
            FillUntilThreeLevelsWithThreeRootChildren(model);

            long[] leafStarts;
            long secondStart, thirdStart, branchPages;
            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                branchPages = list.State.BranchPages;
                var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage)).GetDebugOutput();
                secondStart = root[1].Key;
                thirdStart = root[2].Key;
                leafStarts = root.Take(2)
                    .SelectMany(child => new PostingListBranchPage(list.Llt.GetPage(child.Page)).GetDebugOutput())
                    .Select(leaf => leaf.Key)
                    .ToArray();
            }

            // emptying the first leaf of the second branch leaves its first key above the start of its range,
            // putting the values back splits the next leaf below that key, which moves the first key down to 0
            int secondFirstLeaf = Array.IndexOf(leafStarts, secondStart);
            var firstLeaf = model.GetViewBetween(secondStart, leafStarts[secondFirstLeaf + 1] - 1).ToList();
            Remove(firstLeaf, model);
            Add(firstLeaf, model);
            AssertShape(model);

            // keep every 8th leaf, so both branches get small enough to merge with their first keys as they are.
            // The branch that shrinks last is the one that takes in the other.
            var keep = new HashSet<long>(firstLeaf);
            var pending = model.Where(v => v < thirdStart && keep.Contains(v) == false && LeafIndex(v) % 8 != 0)
                .OrderBy(v => drainTheRightBranchFirst == (v >= secondStart) ? 0 : 1)
                .ThenBy(v => v)
                .ToList();
            for (int i = 0; i < pending.Count; i += 100_000)
            {
                Remove(pending.Skip(i).Take(100_000).ToList(), model);
                AssertShape(model);
            }

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                Assert.True(list.State.BranchPages < branchPages, $"no branches merged, {list.State.BranchPages} branch pages");

                foreach (long value in model.Where(v => v < thirdStart))
                    Assert.Equal(value, FirstAtOrAbove(list, value));
            }

            int LeafIndex(long value)
            {
                int idx = Array.BinarySearch(leafStarts, value);
                return idx >= 0 ? idx : ~idx - 1;
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void WrappingACollapsedFirstChildThatHoldsValuesBelowItsKey()
        {
            var model = new SortedSet<long>();
            FillUntilThreeLevelsWithThreeRootChildren(model);
            var all = model.ToList();

            long secondStart, thirdStart;
            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                secondStart = root.GetByIndex(1).Key;
                thirdStart = root.GetByIndex(2).Key;
            }

            // the second child of the root drains down to its first 100 values and collapses into a leaf. Then the first
            // child is emptied and unlinked, so the collapsed leaf becomes the first child and keeps its key.
            Remove(model.GetViewBetween(secondStart, thirdStart - 1).Skip(100).ToList(), model);
            Remove(model.GetViewBetween(long.MinValue, secondStart - 1).ToList(), model);

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                Assert.Equal(2, root.Header->NumberOfEntries);
                var (firstKey, firstPage) = root.GetByIndex(0);
                Assert.Equal(secondStart, firstKey);
                var header = (PostingListLeafPageHeader*)list.Llt.GetPage(firstPage).Pointer;
                Assert.Equal(ExtendedPageType.PostingListLeaf, header->PageType);
                Assert.True(header->CollapsedLevels > 0, "the first child of the root is not a collapsed leaf");
            }

            // the values below its key overflow the collapsed leaf, so it is wrapped in a branch whose first key is the
            // key the root has for it, and the separator of the new page is below that key
            Add(all.Take(200_000).ToList(), model);
            AssertShape(model);

            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                var wrapper = list.Llt.GetPage(root.GetByIndex(0).Page);
                Assert.Equal(ExtendedPageType.PostingListBranch, ((PostingListLeafPageHeader*)wrapper.Pointer)->PageType);
                Assert.Equal(0, new PostingListBranchPage(wrapper).GetByIndex(0).Key);

                foreach (long value in model.Where((_, idx) => idx % 97 == 0))
                    Assert.Equal(value, FirstAtOrAbove(list, value));
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
        public void EmptiedVectorPostingListFreesItsPages()
        {
            const int Count = 100_000;
            float[] vector = [0.1f, 0.2f, 0.3f, 0.4f];
            var random = new Random(27625);
            byte[] vectorHash = null;

            using (var wtx = Env.WriteTransaction())
            {
                Hnsw.Create(wtx.LowLevelTransaction, "vectors", vector.Length * sizeof(float), 3, 12, VectorEmbeddingType.Single);
                using (var registration = Hnsw.RegistrationFor(wtx.LowLevelTransaction, "vectors", random))
                {
                    for (long i = 1; i <= Count; i++)
                        vectorHash = registration.Register(i << 2, MemoryMarshal.Cast<float, byte>(vector)).ToSpan().ToArray();
                    registration.Commit(CancellationToken.None);
                }

                wtx.Commit();
            }

            List<long> pages;
            using (var rtx = Env.ReadTransaction())
            {
                var lists = rtx.OpenPostingList(Constants.PostingList.PostingListRegister).DumpAllValues();
                Assert.Single(lists);

                Container.Get(rtx.LowLevelTransaction, new ContainerEntryId(lists[0]), out var item);
                var state = MemoryMarshal.Read<PostingListState>(item.ToSpan());
                Assert.Equal(Count, state.NumberOfEntries);
                using (var list = new PostingList(rtx.LowLevelTransaction, Slices.Empty, state))
                    pages = list.AllPages();
            }

            using (var wtx = Env.WriteTransaction())
            {
                using (var registration = Hnsw.RegistrationFor(wtx.LowLevelTransaction, "vectors", random))
                {
                    for (long i = 1; i <= Count; i++)
                        registration.Remove(i << 2, vectorHash);
                    registration.Commit(CancellationToken.None);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                Assert.Empty(rtx.OpenPostingList(Constants.PostingList.PostingListRegister).DumpAllValues());

                var free = Env.FreeSpaceHandling.AllPages(rtx.LowLevelTransaction).ToHashSet();
                var leaked = pages.Where(p => free.Contains(p) == false).ToList();
                Assert.True(leaked.Count == 0, $"the emptied vector posting list leaked {leaked.Count} of its {pages.Count} pages: {string.Join(", ", leaked)}");
            }
        }

        private static long FirstAtOrAbove(PostingList list, long value)
        {
            // Seek lands on the block holding the value, not on the value itself
            var it = list.Iterate();
            if (it.Seek(value) == false)
                return -1;

            Span<long> buffer = stackalloc long[256];
            while (it.Fill(buffer, out int read) && read > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    if (buffer[i] >= value)
                        return buffer[i];
                }
            }

            return -1;
        }

        private long FillUntilThreeLevelsWithThreeRootChildren(SortedSet<long> model)
        {
            long next = Step;
            while (true)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var list = wtx.OpenPostingList(Name);
                    for (int i = 0; i < 100_000; i++, next += Step)
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

                    var root = new PostingListBranchPage(list.Llt.GetPage(list.State.RootPage));
                    if (root.Header->NumberOfEntries >= 3)
                        return root.GetByIndex(root.Header->NumberOfEntries - 1).Key;
                }
            }
        }

        private void AssertShape(SortedSet<long> model)
        {
            using (var rtx = Env.ReadTransaction())
            {
                var list = rtx.OpenPostingList(Name);
                Assert.Equal(model.Count, list.State.NumberOfEntries);

                long leaves = 0, branches = 0;
                Visit(list.Llt.GetPage(list.State.RootPage), isRoot: true, long.MinValue, long.MaxValue);

                Assert.Equal(list.State.LeafPages, leaves);
                Assert.Equal(list.State.BranchPages, branches);
                Assert.Equal(model, list.DumpAllValues());

                void Visit(Page page, bool isRoot, long min, long maxExclusive)
                {
                    var header = (PostingListLeafPageHeader*)page.Pointer;
                    if (header->PageType == ExtendedPageType.PostingListLeaf)
                    {
                        leaves++;
                        var values = new PostingListLeafPage(page).GetDebugOutput();
                        Assert.True(isRoot || values.Count > 0, $"page {page.PageNumber} is an empty leaf that is not the root");
                        Assert.All(values, v => Assert.InRange(v, min, maxExclusive - 1));
                        return;
                    }

                    branches++;
                    var children = new PostingListBranchPage(page).GetDebugOutput();
                    Assert.True(children.Count >= 2, $"branch {page.PageNumber} has {children.Count} children");
                    Assert.True(children.Select(c => c.Key).SequenceEqual(children.Select(c => c.Key).Order()), $"branch {page.PageNumber} is not sorted");
                    // the first key is not a lower bound, the others are the exact lower bounds of their children
                    for (int i = 1; i < children.Count; i++)
                        Assert.InRange(children[i].Key, min, maxExclusive - 1);

                    for (int i = 0; i < children.Count; i++)
                    {
                        var max = i < children.Count - 1 ? children[i + 1].Key : maxExclusive;
                        Visit(list.Llt.GetPage(children[i].Page), isRoot: false, i == 0 ? min : children[i].Key, max);
                    }
                }
            }
        }

        private void Add(List<long> values, SortedSet<long> model)
        {
            using (var wtx = Env.WriteTransaction())
            {
                var list = wtx.OpenPostingList(Name);
                foreach (long value in values)
                {
                    list.Add(value);
                    model.Add(value);
                }

                wtx.Commit();
            }
        }

        private void Remove(List<long> values, SortedSet<long> model)
        {
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
    }
}
