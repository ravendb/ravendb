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
                    Env.RootTree(tx).Add(tx, string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}9", i), new MemoryStream(new byte[8192]));

                    var rootPageNumber = tx.GetTree(Env.RootTree(tx).Name).State.RootPageNumber;
                    var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * (i + 1));
                }

                RenderAndShow(tx, 1);

                for (int i = 79; i >= 0; --i)
                {
                    Env.RootTree(tx).Delete(tx, string.Format("{0}1", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}2", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}3", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}4", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}5", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}6", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}7", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}8", i));
                    Env.RootTree(tx).Delete(tx, string.Format("{0}9", i));

					var rootPageNumber = tx.GetTree(Env.RootTree(tx).Name).State.RootPageNumber;
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
                    Env.RootTree(tx).Add(tx, string.Format("{0}1", i), new MemoryStream(new byte[1472]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}2", i), new MemoryStream(new byte[992]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}3", i), new MemoryStream(new byte[1632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}4", i), new MemoryStream(new byte[632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}6", i), new MemoryStream(new byte[1096]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}7", i), new MemoryStream(new byte[2048]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}8", i), new MemoryStream(new byte[1228]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}9", i), new MemoryStream(new byte[8192]));

					var rootPageNumber = tx.GetTree(Env.RootTree(tx).Name).State.RootPageNumber;
					var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * (i + 1));
                }

                RenderAndShow(tx, 1);

                for (int i = 0; i < 80; ++i)
                {
                    Env.RootTree(tx).Add(tx, string.Format("{0}9", i), new MemoryStream(new byte[1472]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}8", i), new MemoryStream(new byte[992]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}7", i), new MemoryStream(new byte[1632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}6", i), new MemoryStream(new byte[632]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}5", i), new MemoryStream(new byte[824]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}4", i), new MemoryStream(new byte[1096]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}3", i), new MemoryStream(new byte[2048]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}2", i), new MemoryStream(new byte[1228]));
                    Env.RootTree(tx).Add(tx, string.Format("{0}1", i), new MemoryStream(new byte[8192]));

					var rootPageNumber = tx.GetTree(Env.RootTree(tx).Name).State.RootPageNumber;
					var rootPage = tx.Pager.Get(tx, rootPageNumber);
                    Assert.Equal(rootPage.ItemCount, 9 * 80);
                }

                tx.Commit();
            }
        }
    }
}
