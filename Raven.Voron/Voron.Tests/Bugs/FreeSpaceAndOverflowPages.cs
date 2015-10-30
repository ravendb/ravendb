// -----------------------------------------------------------------------
//  <copyright file="T1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron.Debugging;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class FreeSpaceAndOverflowPages : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [PrefixesFact]
        public void ShouldCorrectlyFindSmallValueMergingByTwoSectionsInFreeSpaceHandling()
        {
            var dataSize = 905048; // never change this

            const int itemsCount = 10;

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.Root.Add			("items/" + i, new byte[dataSize]);
                    tx.Commit();
                }

                if(i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {

                for (int i = 0; i < itemsCount; i++)
                {
                    var readResult = tx.Root.Read("items/" + i);

                    Assert.Equal(dataSize, readResult.Reader.Length);
                }
            }

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.Root.Delete("items/" + i);
                    tx.Commit();
                }

                if (i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.Root.Add			("items/" + i, new byte[dataSize]);

                    DebugStuff.RenderAndShow(tx, tx.Root.State.RootPageNumber);

                    tx.Commit();
                }

                if (i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                for (int i = 0; i < itemsCount; i++)
                {
                    var readResult = tx.Root.Read("items/" + i);

                    Assert.Equal(dataSize, readResult.Reader.Length);
                }
            }
        }
    }
}
