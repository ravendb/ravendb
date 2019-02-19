using System.IO;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class CanDefrag : StorageTest
    {
        [Fact]
        public void CanDeleteAtRoot()
        {
            var size = 250;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < size; i++)
                {
                    tree.Add(string.Format("{0,5}", i * 2), StreamFor("abcdefg"));
                }
                tx.Commit();
            }
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < size / 2; i++)
                {
                    tree.Delete(string.Format("{0,5}", i * 2));
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var pageCount = tree.State.PageCount;
                tree.Add("  244", new MemoryStream(new byte[512]));
                Assert.Equal(pageCount, tree.State.PageCount);
                tx.Commit();
            }
        }

    }
}
