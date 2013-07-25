using System.IO;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            new FreeSpace().WillBeReused();

            //var buf = new byte[256*3];

            //using (var storage = new StorageEnvironment(new PureMemoryPager()))
            //{
            //    using (Transaction tx = storage.NewTransaction(TransactionFlags.ReadWrite))
            //    {
            //        for (int i = 0; i < 5000; i++)
            //        {
            //            storage.Root.Add(tx, i.ToString("00000"), new MemoryStream(buf));
            //        }

            //        tx.Commit();
            //    }

            //    using (Transaction tx = storage.NewTransaction(TransactionFlags.Read))
            //    {
            //        DebugStuff.RenderAndShow(tx, storage.Root.Root, 1);
            //    }


            //}
        }
    }
}