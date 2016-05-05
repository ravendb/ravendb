// -----------------------------------------------------------------------
//  <copyright file="T1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Voron;
using Voron.Data;

namespace FastTests.Voron.Bugs
{
    public class FreeSpaceAndOverflowPages : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldCorrectlyFindSmallValueMergingByTwoSectionsInFreeSpaceHandling()
        {
            var dataSize = 905048; // never change this

            const int itemsCount = 10;

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, new byte[dataSize]);
                    tx.Commit();
                }

                if (i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            using (var tx = Env.ReadTransaction())
            {

                var tree = tx.CreateTree("foo");
                for (int i = 0; i < itemsCount; i++)
                {
                    var readResult = tree.Read("items/" + i);

                    Assert.Equal(dataSize, readResult.Reader.Length);
                }
            }

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Delete("items/" + i);
                    tx.Commit();
                }

                if (i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            for (int i = 0; i < itemsCount; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, new byte[dataSize]);

                   
                    tx.Commit();
                }

                if (i % (itemsCount / 3) == 0 || i % (itemsCount / 2) == 0)
                    Env.FlushLogToDataFile();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < itemsCount; i++)
                {
                    var readResult = tree.Read("items/" + i);

                    Assert.Equal(dataSize, readResult.Reader.Length);
                }
            }
        }
    }
}
