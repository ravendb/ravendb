using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class Rebalance : StorageTest
    {
        [Fact]
        public void CanMergeRight()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.Root.Add(tx, "1", new MemoryStream(new byte[1472]));
                Env.Root.Add(tx, "2", new MemoryStream(new byte[992]));
                Env.Root.Add(tx, "3", new MemoryStream(new byte[1632]));
                Env.Root.Add(tx, "4", new MemoryStream(new byte[632]));
                Env.Root.Add(tx, "5", new MemoryStream(new byte[824]));
                Env.Root.Delete(tx, "3");
                Env.Root.Add(tx, "6", new MemoryStream(new byte[1096]));

                RenderAndShow(tx, 1);

                Env.Root.Delete(tx, "6");
                Env.Root.Delete(tx, "4");

                RenderAndShow(tx,1);

                tx.Commit();
            }
        }

        [Fact]
        public void CanMergeLeft()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.Root.Add(tx, "1", new MemoryStream(new byte[1524]));
                Env.Root.Add(tx, "2", new MemoryStream(new byte[1524]));
                Env.Root.Add(tx, "3", new MemoryStream(new byte[1024]));
                Env.Root.Add(tx, "4", new MemoryStream(new byte[64]));

                RenderAndShow(tx, 1);

                Env.Root.Delete(tx, "2");

                RenderAndShow(tx, 1);

				Env.Root.Delete(tx, "3");
				RenderAndShow(tx, 1);

                tx.Commit();
            }
        }

        [Fact]
        public void StressTest()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 80; ++i)
                {
                    Env.Root.Add(tx, string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    Env.Root.Add(tx, string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    Env.Root.Add(tx, string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    Env.Root.Add(tx, string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    Env.Root.Add(tx, string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    Env.Root.Add(tx, string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    Env.Root.Add(tx, string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    Env.Root.Add(tx, string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    Env.Root.Add(tx, string.Format("{0}9", i), new MemoryStream(new byte[8192]));
                }

                RenderAndShow(tx, 1);

                for (int i = 79; i >= 0; --i)
                {
                    Env.Root.Delete(tx, string.Format("{0}1", i));
                    Env.Root.Delete(tx, string.Format("{0}2", i));
                    Env.Root.Delete(tx, string.Format("{0}3", i));
                    Env.Root.Delete(tx, string.Format("{0}4", i));
                    Env.Root.Delete(tx, string.Format("{0}5", i));
                    Env.Root.Delete(tx, string.Format("{0}6", i));
                    Env.Root.Delete(tx, string.Format("{0}7", i));
                    Env.Root.Delete(tx, string.Format("{0}8", i));
                    Env.Root.Delete(tx, string.Format("{0}9", i));
                }

                tx.Commit();
            }
        }
    }
}