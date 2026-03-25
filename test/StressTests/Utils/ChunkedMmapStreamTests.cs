using System.Threading.Tasks;
using FastTests.Utils;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Utils
{
    public class ChunkedMmapStreamStressTests : NoDisposalNoOutputNeeded
    {
        public ChunkedMmapStreamStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(128*1024 + 17, 64)]
        public async Task Can_seek_and_read_from_chunked_mmap_file(int totalSize, int chunkSize, int seed)
        {
            await using (var chunkedMmapStreamTests = new ChunkedMmapStreamTests(Output))
            {
                chunkedMmapStreamTests.Can_seek_and_read_from_chunked_mmap_file(totalSize, chunkSize, seed);
            }
        }
    }
}
