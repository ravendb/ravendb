using System.IO;
using FastTests.Voron;
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_16363 : StorageTest
    {
        public RavenDB_16363(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void MustNotCreateEmptyNonCompressedPageAfterPageSplit()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                var bytes = new byte[3070];

                const int numberOfItems = 21;

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(bytes));
                }

                for (int i = 1; i < numberOfItems; i++) // skipping one item here
                {
                    tree.Delete("items/" + i);
                }

                // after the above deletions there is only one entry on the page - "items/0"

                DebugStuff.RenderAndShow(tree);

                // this is an _update_ (very important)
                // the issue was that the updated entry was added to a new (right) page by TreePageSplitter
                // while the previous entry was removed from compressed page - effectively making it empty
                tree.Add("items/0", new MemoryStream(new byte[3700])); 

                foreach (var pageNumber in tree.AllPages())
                {
                    var page = new TreePage(tx.LowLevelTransaction.GetPage(pageNumber).Pointer, Constants.Storage.PageSize);

                    if (page.IsCompressed == false)
                    {
                        Assert.NotEqual(0, page.NumberOfEntries);
                    }
                }

                Assert.Equal(1, tree.AllPages().Count);
            }
        }
    }
}
