using FastTests;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.Auto
{
    public class RavenDB_9535: NoDisposalNeeded
    {
        [Fact]
        public void Invalid_hash_calculation_on_null()
        {
            using (var bufferPool = new UnmanagedBuffersPoolWithLowMemoryHandling("RavenDB_9535"))
            using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                var sut = new ReduceKeyProcessor(1, bufferPool);

                try
                {
                    sut.Reset();
                    sut.Process(bsc, null);
                    Assert.Equal((ulong)0, sut.Hash);

                    sut.Reset();
                    sut.Process(bsc, 1);
                    Assert.NotEqual((ulong)0, sut.Hash);

                    sut.Reset();
                    sut.Process(bsc, null);
                    Assert.Equal((ulong)0, sut.Hash);
                }
                finally
                {
                    sut.ReleaseBuffer();
                }
            }
        }
    }
}
