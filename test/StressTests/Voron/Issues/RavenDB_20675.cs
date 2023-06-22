using System;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Issues
{
    public unsafe class RavenDB_20675 : StorageTest
    {
        private int _64KB = 64 * 1024;

        public RavenDB_20675(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 2;
        }

        [Fact]
        public void MustNotReusePagesWhichHaveAlreadyDisposedPager()
        {
            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;

                var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp1);
                var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp2);

                Assert.Equal(1, Env.DecompressionBuffers.NumberOfScratchFiles);

                var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp3);

                Assert.Equal(2, Env.DecompressionBuffers.NumberOfScratchFiles);

                page1.Dispose();
                page2.Dispose();
                page3.Dispose();

                Env.DecompressionBuffers.Cleanup();

                // underlying pager of page1 and page2 was disposed during the cleanup
                Assert.Equal(1, Env.DecompressionBuffers.NumberOfScratchFiles);

                // will try to take it from the pool but since the pager of page1 and page2 is already disposed
                // it must not take those pagers so it should reuse page3
                var page4 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp4);

                unsafe
                {
                    Assert.Equal(new IntPtr(temp3), new IntPtr(temp4));
                }
            }
        }

        [Fact]
        public void DonotDisposePagerWhenStillInUse()
        {
            using (var tx = Env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;
                var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp);
                var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);
                var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                page1.Dispose();
                // old pager is still in use with page2
                Assert.Equal(0, Env.DecompressionBuffers.Cleanup());
                page2.Dispose();
                // we dispose the old pager and stay only with one pager
                Assert.Equal(1, Env.DecompressionBuffers.Cleanup());

            }

        }

        [Fact]
        public void CleanupPagerWhenNotInUse()
        {
            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;

                var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp);

                var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                Assert.Equal(2, Env.DecompressionBuffers.NumberOfScratchFiles);

                page1.Dispose();
                page2.Dispose();
                page3.Dispose();

                //cleanup the first pager
                Assert.Equal(1, Env.DecompressionBuffers.Cleanup());
                Assert.Equal(1, Env.DecompressionBuffers.NumberOfScratchFiles);
            }
        }

        [Fact]
        public void UsePagerFromThePool()
        {
            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;

                var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp);

                var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);


                Assert.Equal(2, Env.DecompressionBuffers.NumberOfScratchFiles);

                page1.Dispose();
                page2.Dispose();
                page3.Dispose();

                // will take it from the pool . page1 and page2 not in use but we didn't dispose the pager yet
                var page4 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp);

                // nothing to cleanup
                Assert.Equal(0, Env.DecompressionBuffers.Cleanup());
                Assert.Equal(2, Env.DecompressionBuffers.NumberOfScratchFiles);
            }

        }
    }
}
