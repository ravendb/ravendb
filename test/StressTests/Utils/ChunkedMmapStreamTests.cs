using FastTests;
using FastTests.Utils;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Utils
{
    public class ChunkedMmapStreamStressTests : NoDisposalNoOutputNeeded
    {
        public ChunkedMmapStreamStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineDataWithRandomSeed(128*1024 + 17, 64)]
        public void Can_seek_and_read_from_chunked_mmap_file(int totalSize, int chunkSize, int seed)
        {
            using (var chunkedMmapStreamTests = new ChunkedMmapStreamTests(Output))
            {
                chunkedMmapStreamTests.Can_seek_and_read_from_chunked_mmap_file(totalSize, chunkSize, seed);
            }
        }
    }
}
