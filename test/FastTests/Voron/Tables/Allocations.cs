using System.Linq;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class Allocations : TableStorageTest
    {
        [Fact]
        public void IndexPagesWillBeNearby()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }
            var largeString = new string('a', 1024);
            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                for (int i = 0; i < 2500; i++)
                {
                    SetHelper(docs, "users/" + i, "Users", 1L + i, largeString);
                }

                tx.Commit();
            }


            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                foreach (var index in DocsSchema.Indexes)
                {
                    var tree = docs.GetTree(index.Value);
                    Assert.NotEqual(1, tree.State.Depth);
                    var pages = tree.AllPages();
                    var minPage = pages.Min();
                    var maxPage = pages.Max();
                    Assert.True((maxPage - minPage) < 256);
                }
            }
        }
    }
}