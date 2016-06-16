// -----------------------------------------------------------------------
//  <copyright file="MultiReads.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Bugs
{
    public class MultiReads : StorageTest
    {
        [PrefixesFact]
        public void MultiReadShouldKeepItemOrder()
        {
            foreach (var treeName in CreateTrees(Env, 1, "tree"))
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.ReadTree(treeName).MultiAdd("queue1", "queue1/07000000-0000-0000-0000-000000000001");
                    tx.ReadTree(treeName).MultiAdd("queue1", "queue1/07000000-0000-0000-0000-000000000002");

                    tx.Commit();
                }

                using (var snapshot = Env.CreateSnapshot())
                using (var iterator = snapshot.MultiRead(treeName, "queue1"))
                {
                    Assert.True(iterator.Seek(Slice.BeforeAllKeys));

                    Assert.Equal("queue1/07000000-0000-0000-0000-000000000001", iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());
                    Assert.Equal("queue1/07000000-0000-0000-0000-000000000002", iterator.CurrentKey.ToString());
                    Assert.False(iterator.MoveNext());
                }
            }
        }

        [PrefixesFact]
        public void MultiReadShouldKeepItemOrderWhenInsertingLotOfItems()
        {
            var numberOfEntries = 1000;

            foreach (var treeName in CreateTrees(Env, 1, "tree"))
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (int i = 0; i < numberOfEntries; i++)
                    {
                        tx.ReadTree(treeName).MultiAdd("etags", $"07000000-0000-0000-0000-0000{i:x8}");
                    }

                    tx.Commit();
                }

                using (var snapshot = Env.CreateSnapshot())
                using (var iterator = snapshot.MultiRead(treeName, "etags"))
                {
                    Assert.True(iterator.Seek(Slice.BeforeAllKeys));

                    for (int i = 0; i < numberOfEntries; i++)
                    {
                        Assert.Equal($"07000000-0000-0000-0000-0000{i:x8}", iterator.CurrentKey.ToString());
                        iterator.MoveNext();
                    }
                }
            }
        }
    }
}
