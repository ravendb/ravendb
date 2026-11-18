using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public unsafe class IteratorReadAheadTests(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void AnIteratorReadsAheadOnlyThePagesOfTheEntriesNearItsPosition()
    {
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("items");
            var value = new byte[64];
            for (var i = 0; i < 5_000; i++)
                tree.Add($"items/{i:D8}", value);

            var root = tree.GetReadOnlyTreePage(tree.ReadHeader().RootPageNumber);
            Assert.True(root.IsBranch);
            Assert.True(root.NumberOfEntries > Pager.IteratorReadAheadPages + 4);

            const int window = Pager.IteratorReadAheadPages;
            Assert.Equal(Children(root, 3, window), Read(new TreePage.PagesReferencedEnumerator(root, 3, window)));
            Assert.Equal(Children(root, root.NumberOfEntries - 2, 2), Read(new TreePage.PagesReferencedEnumerator(root, root.NumberOfEntries - 2, window)));
            Assert.Equal(Children(root, 0, 3), Read(new TreePage.PagesReferencedEnumerator(root, 3 - window, window))); // before the first entry
        }
    }

    private static List<long> Children(TreePage page, int first, int count) =>
        Enumerable.Range(first, count).Select(i => page.GetNode(i)->PageNumber).ToList();

    private static List<long> Read(TreePage.PagesReferencedEnumerator enumerator)
    {
        var pages = new List<long>();
        while (enumerator.MoveNext())
            pages.Add(enumerator.Current);
        return pages;
    }
}
