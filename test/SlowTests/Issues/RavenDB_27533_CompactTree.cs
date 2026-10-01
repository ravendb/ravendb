using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Xunit;

namespace SlowTests.Issues;

/// <summary>
/// A CompactTree is a Lookup whose keys live in a terms container, Corax keeps the terms of every field in one.
/// </summary>
public unsafe class RavenDB_27533_CompactTree(ITestOutputHelper output) : StorageTest(output)
{
    private const LookupPageFlags Branch = LookupPageFlags.Branch;
    private const LookupPageFlags Leaf = LookupPageFlags.Leaf;
    private static readonly LookupPageFlags CollapsedLeaf = PageCollapsedLevels.Set(LookupPageFlags.Leaf, 1);

    [RavenFact(RavenTestCategory.Voron)]
    public void SplittingLeafNextToBranchSiblingMustWrapItInBranch()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("terms");

            long next = 0;
            while (tree.BranchPages < 3)
            {
                tree.Add(Key(next), next);
                next++;
            }

            long lastKey = next - 1;
            long rootPage = tree.RootPage;
            Assert.Equal(new[] { Branch, Branch }, ChildPageFlags(tree, rootPage));
            long leafPages = tree.LeafPages;

            // empties the single-entry leaf, which collapses its two-entry parent into a leaf directly under the root
            Assert.True(tree.TryRemove(Key(lastKey), out _));
            Assert.Equal(2, tree.BranchPages);
            Assert.Equal(new[] { Branch, CollapsedLeaf }, ChildPageFlags(tree, rootPage));

            // the collapsed leaf is full, so this append splits it, which wraps it in a branch
            tree.Add(Key(lastKey), lastKey);
            Assert.Equal(3, tree.BranchPages);
            Assert.Equal(leafPages, tree.LeafPages);
            Assert.Equal(new[] { Branch, Branch }, ChildPageFlags(tree, rootPage));
            Assert.Equal(new[] { Leaf, Leaf }, ChildPageFlags(tree, tree.AllEntriesIn(rootPage)[1].Item2));

            Assert.True(tree.TryGetValue(Key(lastKey), out long value));
            Assert.Equal(lastKey, value);
            tree.VerifyStructure();
        }
    }

    private static LookupPageFlags[] ChildPageFlags(CompactTree tree, long pageNumber)
    {
        return tree.AllEntriesIn(pageNumber)
            .Select(x => ((LookupPageHeader*)tree._inner.Llt.GetPage(x.Item2).Pointer)->PageFlags)
            .ToArray();
    }

    private static string Key(long value) => $"term-{value:D12}";
}
