using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Xunit;

namespace SlowTests.Issues;

public unsafe class RavenDB_27533_Tree(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "entries";
    private static readonly byte[] Value = new byte[128];

    /// <summary>
    /// The collapse that RavenDB-27533 is about needs a branch that is down to a single child, which the
    /// rebalancer normally avoids by moving a node over from a sibling or merging the two pages. The
    /// counter is therefore mostly a guard here, so this test covers the churn instead: the tree keeps
    /// its structure and contents, and no page ends up stuck owing levels it never pays back.
    /// </summary>
    [RavenFact(RavenTestCategory.Voron)]
    public void DeletingAndAddingBackKeepsTheTreeConsistent()
    {
        var model = new SortedSet<long>();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree(TreeName);
            tx.Commit();
        }

        long next = 0;
        while (true)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                for (int i = 0; i < 5_000; i++, next++)
                {
                    tree.Add(Key(next), Value);
                    model.Add(next);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                if (tx.ReadTree(TreeName).State.Header.Depth >= 3)
                    break;
            }
        }

        long last = next - 1;
        for (int round = 0; round < 3; round++)
        {
            long from = last - 6_000, to = last - round * 2_000;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                for (long key = from; key <= to; key++)
                {
                    if (model.Remove(key))
                        tree.Delete(Key(key));
                }

                tx.Commit();
            }

            AssertConsistent(model);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                for (long key = from; key <= to; key++)
                {
                    if (model.Add(key))
                        tree.Add(Key(key), Value);
                }

                tx.Commit();
            }

            AssertConsistent(model);
        }
    }

    private void AssertConsistent(SortedSet<long> model)
    {
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            Assert.All(AllPages(tree), p => Assert.True(p.CollapsedLevels <= PageCollapsedLevels.Max,
                $"page {p.PageNumber} is owing {p.CollapsedLevels} levels, more than the cap"));
        }

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MarkedPageMustWrapInPlaceWhenItSplits()
    {
        var model = new SortedSet<long>();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree(TreeName);
            tx.Commit();
        }

        long next = 0;
        while (true)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                for (int i = 0; i < 2_000; i++, next += 2)
                {
                    tree.Add(Key(next), Value);
                    model.Add(next);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                if (tree.State.Header.Depth == 2 && RootChildren(tree).Length >= 3)
                    break;
            }
        }

        long rootPage, markedPage;
        int rootEntries;
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            rootPage = tree.State.Header.RootPageNumber;
            var root = tree.GetReadOnlyTreePage(rootPage);
            rootEntries = root.NumberOfEntries;
            markedPage = root.GetNode(1)->PageNumber;
            Assert.True(tree.GetReadOnlyTreePage(markedPage).IsLeaf);
        }

        // a page that a collapse promoted is one level shallower than its siblings
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            tree.ModifyPage(markedPage).CollapsedLevels = 1;
            tx.Commit();
        }

        // filling that leaf splits it, which must wrap it in a branch under the root rather than add
        // another leaf pointer to the root next to its branch children
        long from, to;
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            var page = tree.GetReadOnlyTreePage(markedPage);
            from = KeyOf(page, 0);
            to = KeyOf(page, page.NumberOfEntries - 1);
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            for (long key = from + 1; key < to; key += 2)
            {
                if (model.Add(key))
                    tree.Add(Key(key), Value);
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            Assert.Equal(rootPage, tree.State.Header.RootPageNumber);

            var root = tree.GetReadOnlyTreePage(rootPage);

            // the split did not add a pointer to the root, it went under a branch that took the slot
            Assert.Equal(rootEntries, root.NumberOfEntries);

            long wrapperPage = root.GetNode(1)->PageNumber;
            Assert.NotEqual(markedPage, wrapperPage);

            var wrapper = tree.GetReadOnlyTreePage(wrapperPage);
            Assert.True(wrapper.IsBranch);
            Assert.Equal(0, wrapper.CollapsedLevels);

            // the page that was marked is now a leaf under the wrapper, next to its split sibling
            var children = ChildrenOf(tree, wrapperPage);
            Assert.Contains(markedPage, children);
            Assert.All(children, child => Assert.True(tree.GetReadOnlyTreePage(child).IsLeaf));
            Assert.Equal(0, tree.GetReadOnlyTreePage(markedPage).CollapsedLevels);
        }

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WrappingAPageThatOwesTwoLevelsMustLeaveItOwingOne()
    {
        var model = new SortedSet<long>();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree(TreeName);
            tx.Commit();
        }

        long next = 0;
        while (true)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                for (int i = 0; i < 2_000; i++, next += 2)
                {
                    tree.Add(Key(next), Value);
                    model.Add(next);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                if (tree.State.Header.Depth == 2 && RootChildren(tree).Length >= 3)
                    break;
            }
        }

        long rootPage, markedPage, from, to;
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            rootPage = tree.State.Header.RootPageNumber;
            markedPage = tree.GetReadOnlyTreePage(rootPage).GetNode(1)->PageNumber;
            var page = tree.GetReadOnlyTreePage(markedPage);
            from = KeyOf(page, 0);
            to = KeyOf(page, page.NumberOfEntries - 1);
        }

        // a page that collapsed twice between its splits is two levels shallower than its siblings
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            tree.ModifyPage(markedPage).CollapsedLevels = 2;
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            for (long key = from + 1; key < to; key += 2)
            {
                if (model.Add(key))
                    tree.Add(Key(key), Value);
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);

            // a wrap restores a single level, so the wrapper is left owing the rest and wraps again on
            // its next split, while the page it wrapped sits next to its split sibling and owes nothing
            long wrapperPage = tree.GetReadOnlyTreePage(rootPage).GetNode(1)->PageNumber;
            var wrapper = tree.GetReadOnlyTreePage(wrapperPage);
            Assert.True(wrapper.IsBranch);
            Assert.Equal(1, wrapper.CollapsedLevels);
            Assert.All(ChildrenOf(tree, wrapperPage), child => Assert.Equal(0, tree.GetReadOnlyTreePage(child).CollapsedLevels));
        }

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PageThatBecomesTheRootMustNotOweLevels()
    {
        var model = new SortedSet<long>();

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree(TreeName);
            tx.Commit();
        }

        long next = 0;
        long markedPage;
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            while (tree.State.Header.Depth < 3)
            {
                tree.Add(Key(next), Value);
                model.Add(next);
                next++;
            }

            tree.Delete(Key(model.Max));
            model.Remove(model.Max);

            var rootChildren = RootChildren(tree);
            Assert.Equal(2, rootChildren.Length);
            Assert.True(tree.GetReadOnlyTreePage(rootChildren[0]).IsBranch);
            Assert.Equal(1, tree.GetReadOnlyTreePage(rootChildren[1]).CollapsedLevels);

            markedPage = ChildrenOf(tree, rootChildren[0])[0];
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            while (tree.GetReadOnlyTreePage(RootChildren(tree)[0]).IsBranch)
            {
                tree.Delete(Key(model.Min));
                model.Remove(model.Min);
            }

            Assert.Equal(markedPage, RootChildren(tree)[0]);
            Assert.Equal(1, tree.GetReadOnlyTreePage(markedPage).CollapsedLevels);

            while (tree.GetReadOnlyTreePage(tree.State.Header.RootPageNumber).IsBranch)
            {
                tree.Delete(Key(model.Min));
                model.Remove(model.Min);
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            Assert.Equal(markedPage, tree.State.Header.RootPageNumber);

            var root = tree.GetReadOnlyTreePage(markedPage);
            Assert.True(root.IsLeaf);
            Assert.True(root.CollapsedLevels == 0, $"the root is owing {root.CollapsedLevels} levels, but it has no siblings to be shallower than");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CollapsingABranchThatOwesALevelMustPassItToItsChild()
    {
        var model = new SortedSet<long>();
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree(TreeName);
            long next = 0;
            while (tree.State.Header.Depth < 4)
            {
                tree.Add(PaddedKey(next), Value);
                model.Add(next);
                next++;
            }

            long rightBranch = RootChildren(tree)[1];
            long promotedBranch = ChildrenOf(tree, rightBranch)[0];
            long firstKeyOfSibling = KeyOf(tree.GetReadOnlyTreePage(rightBranch), 1);
            while (RootChildren(tree)[1] == rightBranch)
            {
                long key = model.GetViewBetween(long.MinValue, firstKeyOfSibling - 1).Max;
                tree.Delete(PaddedKey(key));
                model.Remove(key);
            }

            Assert.Equal(promotedBranch, RootChildren(tree)[1]);
            Assert.Equal(1, tree.GetReadOnlyTreePage(promotedBranch).CollapsedLevels);

            while (ChildrenOf(tree, promotedBranch).Length > 2)
            {
                tree.Delete(PaddedKey(model.Min));
                model.Remove(model.Min);
            }

            long survivor = ChildrenOf(tree, promotedBranch)[0];
            tree.Delete(PaddedKey(model.Max));
            model.Remove(model.Max);

            Assert.Equal(survivor, RootChildren(tree)[1]);
            var page = tree.GetReadOnlyTreePage(survivor);
            Assert.True(page.CollapsedLevels == 2, $"page {survivor} replaced a branch that owed a level, but owes {page.CollapsedLevels} levels instead of 2");
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void MarkedBranchThatOverflowsMustWrapBeforeItSplits()
    {
        var model = new SortedSet<long>();
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree(TreeName);

            // keys far apart, so that there is room between them for the keys added later
            long next = 0;
            while (tree.State.Header.Depth < 3 || RootChildren(tree).Length < 3)
            {
                tree.Add(PaddedKey(next), Value);
                model.Add(next);
                next += 1000;
            }

            long rootPage = tree.State.Header.RootPageNumber;
            var root = tree.GetReadOnlyTreePage(rootPage);
            int rootEntries = root.NumberOfEntries;
            long markedBranch = root.GetNode(0)->PageNumber;
            Assert.True(tree.GetReadOnlyTreePage(markedBranch).IsBranch);

            // pretend a collapse left this whole subtree one level shallower than its siblings
            tree.ModifyPage(markedBranch).CollapsedLevels = 1;

            // adding keys after the first key of its last leaf splits that leaf again and again, until the
            // branch has no room for another pointer and has to split itself: that must wrap it first, not
            // split it next to its siblings in the root
            var lastLeaf = tree.GetReadOnlyTreePage(ChildrenOf(tree, markedBranch)[^1]);
            long fill = KeyOf(lastLeaf, 0) + 1;
            long end = KeyOf(root, 1);
            while (tree.GetReadOnlyTreePage(rootPage).GetNode(0)->PageNumber == markedBranch)
            {
                Assert.True(fill < end, $"the branch {markedBranch} did not split before the range of its last leaf ran out");
                tree.Add(PaddedKey(fill), Value);
                model.Add(fill);
                fill++;
            }

            Assert.Equal(rootPage, tree.State.Header.RootPageNumber);
            root = tree.GetReadOnlyTreePage(rootPage);
            Assert.Equal(rootEntries, root.NumberOfEntries);

            long wrapperPage = root.GetNode(0)->PageNumber;
            var wrapper = tree.GetReadOnlyTreePage(wrapperPage);
            Assert.True(wrapper.IsBranch);
            Assert.Equal(0, wrapper.CollapsedLevels);

            var wrapped = ChildrenOf(tree, wrapperPage);
            Assert.Contains(markedBranch, wrapped);
            Assert.All(wrapped, child =>
            {
                var page = tree.GetReadOnlyTreePage(child);
                Assert.True(page.IsBranch);
                Assert.Equal(0, page.CollapsedLevels);
                Assert.All(ChildrenOf(tree, child), leaf => Assert.True(tree.GetReadOnlyTreePage(leaf).IsLeaf));
            });

            tx.Commit();
        }

        AssertContents(model);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void RebalancingABranchThatOwesALevelMustNotPushLeavesBelowTheTreeDepth()
    {
        var model = new SortedSet<long>();
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree(TreeName);
            long next = 0;
            while (tree.State.Header.Depth < 4)
            {
                tree.Add(PaddedKey(next), Value);
                model.Add(next);
                next++;
            }

            long rightBranch = RootChildren(tree)[1];
            long promotedBranch = ChildrenOf(tree, rightBranch)[0];
            long firstKeyOfSibling = KeyOf(tree.GetReadOnlyTreePage(rightBranch), 1);
            while (RootChildren(tree)[1] == rightBranch)
            {
                long key = model.GetViewBetween(long.MinValue, firstKeyOfSibling - 1).Max;
                tree.Delete(PaddedKey(key));
                model.Remove(key);
            }

            Assert.Equal(promotedBranch, RootChildren(tree)[1]);
            Assert.Equal(1, tree.GetReadOnlyTreePage(promotedBranch).CollapsedLevels);

            while (ChildrenOf(tree, promotedBranch).Length > 2 && ChildrenOf(tree, promotedBranch).All(child => tree.GetReadOnlyTreePage(child).IsLeaf))
            {
                tree.Delete(PaddedKey(model.Max));
                model.Remove(model.Max);
            }

            while (RootChildren(tree)[1] == promotedBranch)
            {
                tree.Add(PaddedKey(next), Value);
                next++;
            }

            int maxLeafDepth = MaxLeafDepth(tree, tree.State.Header.RootPageNumber);
            Assert.True(maxLeafDepth <= tree.State.Header.Depth, $"a leaf sits at depth {maxLeafDepth}, but the tree depth is {tree.State.Header.Depth}");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SplitOfACachedDecompressionMustSeeTheLevelsMarkedAfterItWasCached()
    {
        var model = new SortedSet<long>();
        long branchPage, markedPage;
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree(TreeName, flags: TreeFlags.LeafsCompressed);
            long next = 0;
            while (tree.State.Header.Depth < 3)
            {
                tree.Add(PaddedKey(next), Value);
                model.Add(next);
                next += 2;
            }

            tree.Delete(PaddedKey(model.Max));
            model.Remove(model.Max);

            branchPage = RootChildren(tree)[0];
            markedPage = ChildrenOf(tree, branchPage)[0];
            Assert.True(tree.GetReadOnlyTreePage(markedPage).IsCompressed);
            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            long firstKeyOfLeaf = KeyOf(tree.GetReadOnlyTreePage(tree.State.Header.RootPageNumber), 1);
            long firstKeyOfSiblings = KeyOf(tree.GetReadOnlyTreePage(branchPage), 1);

            long lastKey = model.GetViewBetween(firstKeyOfSiblings, firstKeyOfLeaf - 1).Max;
            foreach (long key in model.GetViewBetween(firstKeyOfSiblings, lastKey - 1).ToList())
            {
                tree.Delete(PaddedKey(key));
                model.Remove(key);
            }

            while (tree.DecompressionsCache.TryGet(markedPage, DecompressionUsage.Write, out _) == false)
            {
                long key = model.GetViewBetween(long.MinValue, firstKeyOfSiblings - 1).Max;
                tree.Delete(PaddedKey(key));
                model.Remove(key);
            }

            tree.Delete(PaddedKey(lastKey));
            model.Remove(lastKey);

            Assert.Equal(markedPage, RootChildren(tree)[0]);
            Assert.Equal(1, tree.GetReadOnlyTreePage(markedPage).CollapsedLevels);
            Assert.True(tree.DecompressionsCache.TryGet(markedPage, DecompressionUsage.Write, out var cached));
            Assert.Equal(0, cached.CollapsedLevels);

            long next = model.GetViewBetween(long.MinValue, firstKeyOfLeaf - 1).Max + 2;
            int rootEntries = RootChildren(tree).Length;
            while (RootChildren(tree).Length == rootEntries && RootChildren(tree)[0] == markedPage)
            {
                Assert.True(next < firstKeyOfLeaf, $"ran out of keys in the range of page {markedPage}");
                Assert.True(tree.DecompressionsCache.TryGet(markedPage, DecompressionUsage.Write, out _), $"the Write decompression of page {markedPage} is not cached anymore");
                tree.Add(PaddedKey(next), Value);
                next += 2;
            }

            Assert.True(tree.GetReadOnlyTreePage(RootChildren(tree)[0]).IsBranch, $"page {markedPage} split next to its sibling instead of wrapping itself in a branch");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WrappingTheRightPageOfASplitMustNotTakeTheSlotOfTheLeftPage()
    {
        RequireFileBasedPager();

        // written by Voron 24 (before RavenDB-27533): a collapse left an unmarked leaf in the root next to a branch, then its splits filled the root with leaves
        using (var stream = typeof(RavenDB_27533_Tree).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27533.unmarked-leaves-next-to-branch.zip"))
            ZipFile.ExtractToDirectory(stream, DataDir);
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            Assert.True(tree.TryRead(SizedKey(1020, 1000), out _), $"key does not exists in raw data");
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            long leftPage = RootChildren(tree)[5];

            tree.Add(SizedKey(1021, 2025), new byte[4052]);

            Assert.True(tree.TryRead(SizedKey(1020, 1000), out _), $"key 1020 is lost, the wrap of the right page took the slot of page {leftPage}");
        }
    }

    private Slice Key(long value)
    {
        Slice.From(Allocator, $"entries/{value:D10}", out Slice key);
        return key;
    }

    private Slice PaddedKey(long value)
    {
        Slice.From(Allocator, $"entries/{value:D10}/" + new string('x', 1980), out Slice key);
        return key;
    }

    private Slice SizedKey(long value, int size)
    {
        Slice.From(Allocator, SizedKeyString(value, size), out Slice key);
        return key;
    }

    private static string SizedKeyString(long value, int size) => $"entries/{value:D10}/".PadRight(size, 'x');

    private static long ParseKey(string key) => long.Parse(key.AsSpan("entries/".Length, 10));

    private long KeyOf(TreePage page, int index)
    {
        using (TreeNodeHeader.ToSlicePtr(Allocator, page.GetNode(index), out Slice slice))
            return ParseKey(slice.ToString());
    }

    private void AssertContents(SortedSet<long> model)
    {
        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree(TreeName);
            Assert.Equal(model.Count, tree.State.Header.NumberOfEntries);

            var actual = new List<long>();
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        actual.Add(ParseKey(it.CurrentKey.ToString()));
                    } while (it.MoveNext());
                }
            }

            Assert.Equal(model.ToList(), actual);
        }
    }

    private static long[] RootChildren(Tree tree)
    {
        return ChildrenOf(tree, tree.State.Header.RootPageNumber);
    }

    private static long[] ChildrenOf(Tree tree, long pageNumber)
    {
        var page = tree.GetReadOnlyTreePage(pageNumber);
        if (page.IsBranch == false)
            return [];

        var children = new long[page.NumberOfEntries];
        for (int i = 0; i < children.Length; i++)
            children[i] = page.GetNode(i)->PageNumber;
        return children;
    }

    private static int MaxLeafDepth(Tree tree, long pageNumber)
    {
        long[] children = ChildrenOf(tree, pageNumber);
        return children.Length == 0 ? 1 : 1 + children.Max(child => MaxLeafDepth(tree, child));
    }

    private static List<TreePage> AllPages(Tree tree)
    {
        return tree.AllPages().Select(tree.GetReadOnlyTreePage).ToList();
    }
}
