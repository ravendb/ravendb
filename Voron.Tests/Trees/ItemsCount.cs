using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class ItemCount : StorageTest
    {
        [Fact]
        public void ItemCountIsConsistentWithAdditionsAndRemovals()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i=0; i < 80; ++i)
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

                    var rootPageNumber = tx.GetTreeInformation(Env.Root).RootPageNumber;
                    var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * (i + 1));
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

                    var rootPageNumber = tx.GetTreeInformation(Env.Root).RootPageNumber;
                    var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * i);
                }

                tx.Commit();
            }
        }

        [Fact]
        public void ItemCountIsConsistentWithUpdates()
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

                    var rootPageNumber = tx.GetTreeInformation(Env.Root).RootPageNumber;
                    var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * (i + 1));
                }

                RenderAndShow(tx, 1);

                for (int i = 0; i < 80; ++i)
                {
                    Env.Root.Add(tx, string.Format("{0}9", i), new MemoryStream(new byte[1472]));
                    Env.Root.Add(tx, string.Format("{0}8", i), new MemoryStream(new byte[992]));
                    Env.Root.Add(tx, string.Format("{0}7", i), new MemoryStream(new byte[1632]));
                    Env.Root.Add(tx, string.Format("{0}6", i), new MemoryStream(new byte[632]));
                    Env.Root.Add(tx, string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    Env.Root.Add(tx, string.Format("{0}4", i), new MemoryStream(new byte[1096]));
                    Env.Root.Add(tx, string.Format("{0}3", i), new MemoryStream(new byte[2048]));
                    Env.Root.Add(tx, string.Format("{0}2", i), new MemoryStream(new byte[1228]));
                    Env.Root.Add(tx, string.Format("{0}1", i), new MemoryStream(new byte[8192]));

                    var rootPageNumber = tx.GetTreeInformation(Env.Root).RootPageNumber;
                    var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * 80);
                }

                tx.Commit();
            }
        }
    }
}
