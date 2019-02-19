using Voron;
using Voron.Impl.Paging;
using Xunit;
using Assert = Xunit.Assert;

namespace FastTests.Voron.LeafsCompression
{
    public class RavenDB_5384_DecompressionBuffersPoolTests : StorageTest
    {
        private const int _64KB = 64 * 1024;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 4;
        }

        [Fact]
        public void Should_cleanup_scratch_files_which_are_no_longer_necessary()
        {
            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;
                
                TemporaryPage temp;
                TemporaryPage temp3;
                var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp3);

                var page4 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                var page5 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                Assert.Equal(2, Env.DecompressionBuffers.NumberOfScratchFiles);

                page1.Dispose();
                page2.Dispose();
                
                Env.DecompressionBuffers.Cleanup();

                Assert.Equal(1, Env.DecompressionBuffers.NumberOfScratchFiles);

                Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);
            }
            
        }
    }
}