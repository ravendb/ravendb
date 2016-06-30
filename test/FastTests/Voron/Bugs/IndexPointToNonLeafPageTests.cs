// -----------------------------------------------------------------------
//  <copyright file="IndexPointToNotLeafPageTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron;
using Voron.Data;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class IndexPointToNonLeafPageTests : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldProperlyMovePositionForNextPageAllocationInScratchBufferPool()
        {
            var sequentialLargeIds = TestDataUtil.ReadData("non-leaf-page-seq-id-large-values.txt");

            var enumerator = sequentialLargeIds.GetEnumerator();

            for (var transactions = 0; transactions < 36; transactions++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (var i = 0; i < 100; i++)
                    {
                        enumerator.MoveNext();

                        tree.Add(enumerator.Current.Key.ToString("0000000000000000"), new MemoryStream(enumerator.Current.Value));
                    }

                    tx.Commit();
                }

                Env.FlushLogToDataFile();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                foreach (var item in sequentialLargeIds)
                {
                    var readResult = tree.Read(item.Key.ToString("0000000000000000"));

                    Assert.NotNull(readResult);

                    Assert.Equal(item.Value.Length, readResult.Reader.Length);
                }
            }
        }
    }
}