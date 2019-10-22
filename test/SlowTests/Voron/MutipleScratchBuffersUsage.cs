// -----------------------------------------------------------------------
//  <copyright file="MutipleScratchBuffersHandling.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests.Voron;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class MutipleScratchBuffersUsage : StorageTest
    {
        public MutipleScratchBuffersUsage(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = 1024 * 1024 * 12; // 2048 pages
            options.MaxNumberOfPagesInJournalBeforeFlush = 96;
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanAddContinuallyGrowingValue()
        {
            // this test does not have any assertion - we just check here that we don't get ScratchBufferSizeLimitException

            var size = 0;

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("foo");

                txw.Commit();
            }

            // note that we cannot assume that we can't take the whole scratch because there is always a read transaction
            // also in the meanwhile the free space handling is doing its job so it needs some pages too
            // and we allocate not the exact size but the nearest power of two e.g. we write 257 pages but in scratch we request 512 ones
            int i = 0;
            while (size < 256 * Constants.Storage.PageSize)
            {
                using (Env.ReadTransaction())
                {
                    using (var txw = Env.WriteTransaction())
                    {
                        var tree = txw.ReadTree("foo");

                        tree.Add("item", new byte[size]);

                        txw.Commit();
                    }
                }
                if (i++ % 4 == 0)
                    Env.FlushLogToDataFile();

                size += 1024;
            }
        }

        [Fact]
        public void CanAddContinuallyGrowingValue_ButNotCommitting()
        {
            // this test does not have any assertion - we just check here that we don't get ScratchBufferSizeLimitException

            var size = 0;

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("foo");

                txw.Commit();
            }

            // note that we cannot assume that we can't take the whole scratch because there is always a read transaction
            // also in the meanwhile the free space handling is doing its job so it needs some pages too
            // and we allocate not the exact size but the nearest power of two e.g. we write 257 pages but in scratch we request 512 ones

            while (size < 1024 * Constants.Storage.PageSize)
            {
                using (var txr = Env.ReadTransaction())
                {
                    using (var txw = Env.WriteTransaction())
                    {
                        var tree = txw.ReadTree("foo");

                        tree.Add("item", new byte[size]);

                        // txw.Commit(); - intentionally not committing
                    }
                }

                size += 1024;
            }
        }
    }
}
