using FastTests;
using Xunit;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_5384 : NoDisposalNeeded
    {
        [Theory]
        [InlineData(8192, 512, true, 1)]
        [InlineData(16384, 512, false, 1)]
        public void Leafs_compressed_CRUD(int iterationCount, int size, bool sequentialKeys, int seed)
        {
            using (var test = new FastTests.Voron.LeafsCompression.RavenDB_5384())
            {
                test.Leafs_compressed_CRUD(iterationCount, size, sequentialKeys, seed);
            }
        }
    }
}