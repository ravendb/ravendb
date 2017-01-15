using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Xunit;

namespace SlowTests.Utils
{
    public class SlowChunkedMmapStreamTests : NoDisposalNeeded
    {
        [Theory]
        [FastTests.Voron.FixedSize.InlineDataWithRandomSeed(128*1024 + 17, 64)]
        public void Can_seek_and_read_from_chunked_mmap_file(int totalSize, int chunkSize, int seed)
        {
            using (var chunkedMmapStreamTests = new ChunkedMmapStreamTests())
            {
                chunkedMmapStreamTests.Can_seek_and_read_from_chunked_mmap_file(totalSize, chunkSize, seed);
            }
        }
    }
}
