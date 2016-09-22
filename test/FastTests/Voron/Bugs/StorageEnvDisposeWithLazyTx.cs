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

                    var tree = tx.FixedTreeFor(Slice.From(StorageEnvironment.LabelsContext, "World"), 8);
                    tree.Add(1, Slice.From(StorageEnvironment.LabelsContext, "Hello123"));
                    tx.Commit();
                }
                // delibrately not commiting non-lazy-tx, should not throw
            }
        }
    }
}