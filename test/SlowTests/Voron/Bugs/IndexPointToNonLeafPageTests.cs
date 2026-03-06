using System.IO;
using SlowTests.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class IndexPointToNonLeafPageTests : FastTests.Voron.StorageTest
    {
        public IndexPointToNonLeafPageTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
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
                    Assert.True(tree.TryRead(item.Key.ToString("0000000000000000"), out var reader));
                    Assert.Equal(item.Value.Length, reader.Length);
                }
            }
        }
    }
}
