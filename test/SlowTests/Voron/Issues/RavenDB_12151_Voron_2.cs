using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12151_Voron_2 : StorageTest
    {
        public RavenDB_12151_Voron_2(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TestScratchSpaceMonitor _scratchSpaceMonitor = new TestScratchSpaceMonitor();

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 2 * Constants.Size.Megabyte;
            options.ScratchSpaceUsage.AddMonitor(_scratchSpaceMonitor);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanTrackScratchSpaceSize()
        {
            RequireFileBasedPager();

            var r = new Random(1);

            var bytes = new byte[1024];

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            var temp = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).TempPath.FullPath;

            var scratches = new DirectoryInfo(temp).GetFiles("scratch.*");

            var totalSize = scratches.Sum(x => x.Length);

            Assert.Equal(totalSize, _scratchSpaceMonitor.Size);
            Assert.Equal(totalSize, Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes);
            Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);

            Env.FlushLogToDataFile();
            Env.ForceSyncDataFile();
            Env.Cleanup();

            Assert.True(_scratchSpaceMonitor.Size < totalSize);
            Assert.True(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes < totalSize);
            Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);

            // disposed scratch files can still be listed for a while on Windows (FILE_FLAG_DELETE_ON_CLOSE keeps them until every handle, e.g. AV scanner, indexer, is closed)
            totalSize = WaitForSizeOnDiskToMatch(temp, _scratchSpaceMonitor.Size, "scratch.*");

            Assert.Equal(totalSize, _scratchSpaceMonitor.Size);
            Assert.Equal(totalSize, Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes);
            Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            Env.Dispose();

            Assert.Equal(0, _scratchSpaceMonitor.Size);
            Assert.Equal(0, Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes);
        }

        private static long WaitForSizeOnDiskToMatch(string directory, long expectedSize, params string[] searchPatterns)
        {
            var sw = Stopwatch.StartNew();

            while (true)
            {
                var sizeOnDisk = searchPatterns.Sum(pattern => new DirectoryInfo(directory).GetFiles(pattern).Sum(x => x.Length));

                if (sizeOnDisk == expectedSize || sw.Elapsed > TimeSpan.FromSeconds(30))
                    return sizeOnDisk;

                Thread.Sleep(50);
            }
        }

        private class TestScratchSpaceMonitor : IScratchSpaceMonitor
        {
            public long Size;

            public void Increase(long allocatedScratchSpaceInBytes)
            {
                Size += allocatedScratchSpaceInBytes;
            }

            public void Decrease(long releasedScratchSpaceInBytes)
            {
                Size -= releasedScratchSpaceInBytes;
            }
        }
    }
}
