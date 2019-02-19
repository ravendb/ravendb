using System.IO;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class ItemCount : StorageTest
    {
        [Fact]
        public void ItemCountIsConsistentWithAdditionsAndRemovals()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 80; ++i)
                {
                    tree.Add(string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    tree.Add(string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    tree.Add(string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    tree.Add(string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    tree.Add(string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    tree.Add(string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    tree.Add(string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    tree.Add(string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    tree.Add(string.Format("{0}9", i), new MemoryStream(new byte[8192]));

                    Assert.Equal(tree.State.NumberOfEntries, 9 * (i + 1));
                }


                for (int i = 79; i >= 0; --i)
                {
                    tree.Delete(string.Format("{0}1", i));
                    tree.Delete(string.Format("{0}2", i));
                    tree.Delete(string.Format("{0}3", i));
                    tree.Delete(string.Format("{0}4", i));
                    tree.Delete(string.Format("{0}5", i));
                    tree.Delete(string.Format("{0}6", i));
                    tree.Delete(string.Format("{0}7", i));
                    tree.Delete(string.Format("{0}8", i));
                    tree.Delete(string.Format("{0}9", i));

                    Assert.Equal(tree.State.NumberOfEntries, 9 * i);
                }

                tx.Commit();
            }
        }

        [Fact]
        public void ItemCountIsConsistentWithUpdates()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 80; ++i)
                {
                    tree.Add(string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    tree.Add(string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    tree.Add(string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    tree.Add(string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    tree.Add(string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    tree.Add(string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    tree.Add(string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    tree.Add(string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    tree.Add(string.Format("{0}9", i), new MemoryStream(new byte[8192]));

                    Assert.Equal(tree.State.NumberOfEntries, 9 * (i + 1));
                }

                //RenderAndShow(tx, 1);

                for (int i = 0; i < 80; ++i)
                {
                    tree.Add(string.Format("{0}9", i), new MemoryStream(new byte[1472]));
                    tree.Add(string.Format("{0}8", i), new MemoryStream(new byte[992]));
                    tree.Add(string.Format("{0}7", i), new MemoryStream(new byte[1632]));
                    tree.Add(string.Format("{0}6", i), new MemoryStream(new byte[632]));
                    tree.Add(string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    tree.Add(string.Format("{0}4", i), new MemoryStream(new byte[1096]));
                    tree.Add(string.Format("{0}3", i), new MemoryStream(new byte[2048]));
                    tree.Add(string.Format("{0}2", i), new MemoryStream(new byte[1228]));
                    tree.Add(string.Format("{0}1", i), new MemoryStream(new byte[8192]));

                    Assert.Equal(tree.State.NumberOfEntries, 9 * 80);
                }

                tx.Commit();
            }
        }
    }
}
