using System.IO;
using FastTests.Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.LeafsCompression
{
    public class RDBS_52 : StorageTest
    {
        public RDBS_52(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void MustNotCreateEmptyNonCompressedPage()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Header.Flags.HasFlag(TreeFlags.LeafsCompressed));
                
                var bytes = new byte[3070];

                const int numberOfItems = 21;

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(bytes));
                }

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Delete("items/" + i);
                }
                
                tree.Add("newItem", new MemoryStream(new byte[3700]));

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
