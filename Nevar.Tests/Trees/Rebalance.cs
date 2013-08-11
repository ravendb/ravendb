using System.IO;
using Xunit;

namespace Nevar.Tests.Trees
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

                 tx.Commit();
             }
         }
    }
}