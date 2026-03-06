using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class FreeSpaceAndOverflowPages : FastTests.Voron.StorageTest
    {
        public FreeSpaceAndOverflowPages(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
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
                    Assert.True(tree.TryRead("items/" + i, out var reader));
                    Assert.Equal(dataSize, reader.Length);
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
                    Assert.True(tree.TryRead("items/" + i, out var reader));
                    Assert.Equal(dataSize, reader.Length);
                }
            }
        }
    }
}
