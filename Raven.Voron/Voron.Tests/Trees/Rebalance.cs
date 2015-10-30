using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class Rebalance : StorageTest
    {
        [PrefixesFact]
        public void CanMergeRight()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add("1", new MemoryStream(new byte[1472]));
                tx.Root.Add("2", new MemoryStream(new byte[992]));
                tx.Root.Add("3", new MemoryStream(new byte[1632]));
                tx.Root.Add("4", new MemoryStream(new byte[632]));
                tx.Root.Add("5", new MemoryStream(new byte[824]));
                tx.Root.Delete("3");
                tx.Root.Add("6", new MemoryStream(new byte[1096]));

                RenderAndShow(tx, 1);

                tx.Root.Delete("6");
                tx.Root.Delete("4");

                RenderAndShow(tx, 1);

                tx.Commit();
            }
        }

        [PrefixesFact]
        public void CanMergeLeft()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add("1", new MemoryStream(new byte[1524]));
                tx.Root.Add("2", new MemoryStream(new byte[1524]));
                tx.Root.Add("3", new MemoryStream(new byte[1024]));
                tx.Root.Add("4", new MemoryStream(new byte[64]));

                RenderAndShow(tx, 1);

                tx.Root.Delete("2");

                RenderAndShow(tx, 1);

                tx.Root.Delete("3");
                RenderAndShow(tx, 1);

                tx.Commit();
            }
        }

        [PrefixesFact]
        public void StressTest()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 80; ++i)
                {
                    tx.Root.Add(string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    tx.Root.Add(string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    tx.Root.Add(string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    tx.Root.Add(string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    tx.Root.Add(string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    tx.Root.Add(string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    tx.Root.Add(string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    tx.Root.Add(string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    tx.Root.Add(string.Format("{0}9", i), new MemoryStream(new byte[8192]));
                }

                RenderAndShow(tx, 1);

                for (int i = 79; i >= 0; --i)
                {
                    tx.Root.Delete(string.Format("{0}1", i));
                    tx.Root.Delete(string.Format("{0}2", i));
                    tx.Root.Delete(string.Format("{0}3", i));
                    tx.Root.Delete(string.Format("{0}4", i));
                    tx.Root.Delete(string.Format("{0}5", i));
                    tx.Root.Delete(string.Format("{0}6", i));
                    tx.Root.Delete(string.Format("{0}7", i));
                    tx.Root.Delete(string.Format("{0}8", i));
                    tx.Root.Delete(string.Format("{0}9", i));
                }

                tx.Commit();
            }
        }
    }
}
