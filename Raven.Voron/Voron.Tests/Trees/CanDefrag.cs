using System;
using System.Collections.Generic;
using System.IO;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Trees
{
    public class CanDefrag : StorageTest
    {
        [Fact]
        public void CanDeleteAtRoot()
        {
            var size = 250;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < size; i++)
                {
                    tx.State.Root.Add(tx, string.Format("{0,5}", i*2), StreamFor("abcdefg"));
                }
                tx.Commit();
            }
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < size/2; i++)
                {
                    tx.State.Root.Delete(tx, string.Format("{0,5}", i*2));
                }
                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var pageCount = tx.State.Root.State.PageCount;
                tx.State.Root.Add(tx,  "  244",new MemoryStream(new byte[512]));
                Assert.Equal(pageCount, tx.State.Root.State.PageCount);
                tx.Commit();
            }
        }

    }
}