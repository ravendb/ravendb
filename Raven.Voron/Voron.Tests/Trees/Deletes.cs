using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Trees
{
    public class Deletes : StorageTest
    {

        [PrefixesFact]
        public void CanAddVeryLargeValueAndThenDeleteIt()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("a", new MemoryStream(buffer));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Equal(4, tx.Root.State.PageCount);
                Assert.Equal(3, tx.Root.State.OverflowPages);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Delete("a");

                tx.Commit();
            }


            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Equal(1, tx.Root.State.PageCount);
                Assert.Equal(0, tx.Root.State.OverflowPages);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Null(tx.Root.Read("a"));

                tx.Commit();
            }

            
        }

         [PrefixesFact]
         public void CanDeleteAtRoot()
         {
             using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
             {
                 for (int i = 0; i < 1000; i++)
                 {
                     tx.Root.Add			(string.Format("{0,5}",i), StreamFor("abcdefg"));
                 }
                 tx.Commit();
             }

             var expected = new List<Slice>();
             for (int i = 15; i < 1000; i++)
             {
                 expected.Add(string.Format("{0,5}", i));
             }

             using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
             {
                 for (int i = 0; i < 15; i++)
                 {
                     tx.Root.Delete(string.Format("{0,5}", i));
                 }
                 tx.Commit();
             }


             using (var tx = Env.NewTransaction(TransactionFlags.Read))
             {
                 var list = Keys(tx.Root, tx);
                 Assert.Equal(expected, list);
             }
         }

        public unsafe List<Slice> Keys(Tree t, Transaction tx)
        {
            var results = new List<Slice>();
            using (var it = t.Iterate())
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                    return results;
                do
                {
                    results.Add(it.CurrentKey);
                } while (it.MoveNext());
            }
            return results;
        }
    }
}
