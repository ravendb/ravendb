using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

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

    private Slice Key(long value)
    {
        Slice.From(Allocator, $"entries/{value:D10}", out Slice key);
        return key;
    }

    private long KeyOf(TreePage page, int index)
    {
        using (TreeNodeHeader.ToSlicePtr(Allocator, page.GetNode(index), out Slice slice))
            return long.Parse(slice.ToString()["entries/".Length..]);
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
                        actual.Add(long.Parse(it.CurrentKey.ToString()["entries/".Length..]));
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

    private static List<TreePage> AllPages(Tree tree)
    {
        return tree.AllPages().Select(tree.GetReadOnlyTreePage).ToList();
    }
}
