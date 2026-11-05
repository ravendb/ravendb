using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.Paging
{
    // Reading must give the same data whatever the prefetching does.
    public class PagePrefetchTests(ITestOutputHelper output) : StorageTest(output)
    {
        private const int Items = 12_000;
        private const int ValueSize = 1000; // about 12 MB: the data file is 16 MB, 4 segments

        [RavenFact(RavenTestCategory.Voron)]
        public void TheTestsRunOnThePagerThatPrefetches()
        {
            RequireFileBasedPager();
            Write();
            RestartDatabase();

            Assert.True(global::Sparrow.Platform.PlatformDetails.CanPrefetch, "Prefetching is not supported here: these tests would not test it");
            Assert.Equal("RvnMemoryMapPager", Env.Options.DataPager.GetType().Name);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ReadingTheWholeFileGivesTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write();
            RestartDatabase(); // new pagers: nothing has been read or prefetched

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("data");
                for (var i = 0; i < Items; i++)
                    Verify(tree, i);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ReadingAtRandomGivesTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write();
            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("data");
                var random = new Random(1);
                for (var n = 0; n < Items; n++)
                    Verify(tree, random.Next(Items));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingFromSeveralThreadsAtTheSameTimeGivesTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write();
            RestartDatabase();

            var readers = new Task[6];
            for (var t = 0; t < readers.Length; t++)
            {
                var seed = t;
                readers[t] = Task.Run(() =>
                {
                    using var tx = Env.ReadTransaction();
                    var tree = tx.ReadTree("data");
                    var random = new Random(seed);
                    // half of the readers scan, the others read at random
                    var start = seed % 2 == 0 ? random.Next(Items) : -1;
                    for (var n = 0; n < Items; n++)
                        Verify(tree, start >= 0 ? (start + n) % Items : random.Next(Items));
                });
            }

            Task.WaitAll(readers);
        }

        private void Write()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("data");
                var value = new byte[ValueSize];
                for (var i = 0; i < Items; i++)
                {
                    BitConverter.TryWriteBytes(value, i); // the value of an item says which item it is
                    tree.Add(Key(i), value);
                }

                tx.Commit();
            }
        }

        private static string Key(int i) => "items/" + i.ToString("D8");

        private static unsafe void Verify(global::Voron.Data.BTrees.Tree tree, int i)
        {
            Assert.True(tree.TryRead(Key(i), out var reader), "Item " + i + " was not found");
            Assert.Equal(ValueSize, reader.Length);
            Assert.Equal(i, *(int*)reader.Base);
        }
    }
}
