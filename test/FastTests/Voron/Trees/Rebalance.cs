using System.IO;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class Rebalance : StorageTest
    {
        [Fact]
        public void CanMergeRight()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                tree.Add("1", new MemoryStream(new byte[1472]));
                tree.Add("2", new MemoryStream(new byte[992]));
                tree.Add("3", new MemoryStream(new byte[1632]));
                tree.Add("4", new MemoryStream(new byte[632]));
                tree.Add("5", new MemoryStream(new byte[824]));
                tree.Delete("3");
                tree.Add("6", new MemoryStream(new byte[1096]));

                tree.Delete("6");
                tree.Delete("4");

             
                tx.Commit();
            }
        }

        [Fact]
        public void CanMergeLeft()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                tree.Add("1", new MemoryStream(new byte[1524]));
                tree.Add("2", new MemoryStream(new byte[1524]));
                tree.Add("3", new MemoryStream(new byte[1024]));
                tree.Add("4", new MemoryStream(new byte[64]));
                tree.Delete("2");
                tree.Delete("3");

                tx.Commit();
            }
        }

        [Fact]
        public void StressTest()
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
                }

                tx.Commit();
            }
        }
    }
}
