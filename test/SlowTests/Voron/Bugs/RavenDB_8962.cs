using Voron.Data.BTrees;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_8962 : FastTests.Voron.StorageTest
    {
        public RavenDB_8962(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_not_throw_if_didnt_find_exact_match_when_looking_for_parent_of_branch()
        {
            var key = "test" + new string('-', 256);

            int numberOfItems = 0;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                int size = 0;
                var value = new byte[256];

                while (size < 128 * Constants.Storage.PageSize)
                {
                    numberOfItems++;
                    var s = key + numberOfItems;
                    tree.Add(s, value);
                    size += Tree.CalcSizeOfEmbeddedEntry(s.Length, 256);
                }

                Assert.Equal(3, tree.ReadHeader().Depth);

                var allPages = tree.AllPages();

                long branchPageNumber = -1;

                foreach (var pageNumber in allPages)
                {
                    var treePage = tree.GetReadOnlyTreePage(pageNumber);
                    if (pageNumber != tree.ReadHeader().RootPageNumber && treePage.TreeFlags == TreePageFlags.Branch)
                    {
                        branchPageNumber = pageNumber;
                        break;
                    }
                }

                Assert.NotEqual(-1, branchPageNumber);

                var branchPage = tree.GetReadOnlyTreePage(branchPageNumber);

                var parent = tree.GetParentPageOf(branchPage);

                Assert.Equal(tree.ReadHeader().RootPageNumber, parent);

                using (branchPage.GetNodeKey(tx.LowLevelTransaction, 1, out var nodeKey))
                {
                    tree.Delete(nodeKey);
                }

                parent = tree.GetParentPageOf(branchPage);

                Assert.Equal(tree.ReadHeader().RootPageNumber, parent);
            }
        }
    }
}
