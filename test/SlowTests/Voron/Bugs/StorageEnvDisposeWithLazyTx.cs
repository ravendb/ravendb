// -----------------------------------------------------------------------
//  <copyright file="StorageEnvDisposeWithLazyTx.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Voron;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class StorageEnvDisposeWithLazyTx : NoDisposalNeeded
    {
        [Fact]
        public void CanDisposeStorageWithLazyTx()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.LowLevelTransaction.IsLazyTransaction = true;
                    Slice fst;
                    Slice.From(tx.Allocator, "World", out fst);
                    var tree = tx.FixedTreeFor(fst, 8);
                    Slice val;
                    Slice.From(tx.Allocator, "Hello123", out val);
                    tree.Add(1, val);
                    tx.Commit();
                }
                // delibrately not commiting non-lazy-tx, should not throw
            }
        }
    }
}
