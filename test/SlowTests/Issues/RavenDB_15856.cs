using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15856 : StorageTest
    {
        private int _64KB = 64 * 1024;

        public RavenDB_15856(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 4;
        }

        [Fact]
        public void CanCleanupAndGetTempPagesConcurrentlyFromDecompressionBuffers()
        {
            var run = true;
            var task = Task.Factory.StartNew(() =>
            {
                while (run)
                {
                    Env.DecompressionBuffers.Cleanup();

                    Thread.Sleep(75);
                }
            });

            try
            {
                for (var x = 0; x < 1_000; x++)
                {
                    using (var tx = Env.WriteTransaction())
                    {
                        var llt = tx.LowLevelTransaction;

                        for (var i = 0; i < 50; i++)
                        {
                            using (var page1 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out var temp))
                            using (var page2 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp))
                            using (var page3 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp))
                            using (var page4 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp))
                            using (var page5 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp))
                            using (var page6 = Env.DecompressionBuffers.GetTemporaryPage(llt, _64KB, out temp))
                            {
                            }
                        }
                    }
                }
            }
            finally
            {
                run = false;
                try
                {
                    task.Wait();
                }
                catch (Exception)
                {
                    // ignore
                }
            }
        }
    }
}
