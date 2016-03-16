// -----------------------------------------------------------------------
//  <copyright file="OverflowsReusage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Bugs
{
    public class OverflowsReusage : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [PrefixesTheory]
        [InlineData(2500, 2400)]
        [InlineData(7000, 5000)]
        [InlineData(9000, 2000)]
        public void FaultyOptimization_ReadTransactionCannotSeeUncommittedValue(int overflowSize1, int overflowSize2)
        {
            var r = new Random();
            var bytes1 = new byte[overflowSize1];
            r.NextBytes(bytes1);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "tree");

                tree.Add("key", bytes1);

                tx.Commit();
            }

            Env.FlushLogToDataFile();
            
            var bytes2 = new byte[overflowSize2];
            r.NextBytes(bytes2);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("tree");

                tree.Add("key", bytes2); // TryOverwriteOverflowPages will be used under the hood 

                using (var readTransaction = Env.NewTransaction(TransactionFlags.Read))
                {
                    var readTree = readTransaction.ReadTree("tree");

                    var read = readTree.Read("key").Reader.AsStream().ReadData();

                    Assert.Equal(bytes1, read); // bytes2 isn't committed yet
                }

                tx.Commit();
            }
        }

        [PrefixesTheory]
        [InlineData(2500, 2400)]
        [InlineData(7000, 5000)]
        [InlineData(9000, 2000)]
        public void FaultyOptimization_OverflowPageIsntFlushedToJournal(int overflowSize1, int overflowSize2)
        {
            RequireFileBasedPager();

            var r = new Random();
            var bytes1 = new byte[overflowSize1];
            r.NextBytes(bytes1);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "tree");

                tree.Add("key", bytes1);

                tx.Commit();
            }

            var bytes2 = new byte[overflowSize2];
            r.NextBytes(bytes2);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("tree");

                tree.Add("key", bytes2); // TryOverwriteOverflowPages will be used under the hood 

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var tree = tx.ReadTree("tree");

                var read = tree.Read("key").Reader.AsStream().ReadData();

                Assert.Equal(bytes2, read);
            }
        }
    }
}