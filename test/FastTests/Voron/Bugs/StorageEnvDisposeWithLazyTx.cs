// -----------------------------------------------------------------------
//  <copyright file="StorageEnvDisposeWithLazyTx.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Logging;
using Xunit;
using Voron;

namespace FastTests.Voron.Bugs
{
    public class StorageEnvDisposeWithLazyTx
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
                    Slice.From(StorageEnvironment.LabelsContext, "World", out fst);
                    var tree = tx.FixedTreeFor(fst, 8);
                    Slice val;
                    Slice.From(StorageEnvironment.LabelsContext, "Hello123", out val);
                    tree.Add(1, val);
                    tx.Commit();
                }
                // delibrately not commiting non-lazy-tx, should not throw
            }
        }
    }
}