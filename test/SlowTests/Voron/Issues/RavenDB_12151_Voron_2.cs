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

            // disposed scratch files can still be listed for a while on Windows, see the comment in ScratchSpaceSizeMustIncludeDecompressionBuffers
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

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ScratchSpaceSizeMustIncludeDecompressionBuffers()
        {
            RequireFileBasedPager();
            var temp = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).TempPath.FullPath;

            long totalSize;
            FileInfo[] scratchBuffers;
            FileInfo[] decompressionBuffers;

            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;
                var decompressedTempPagesToDispose = new List<IDisposable>();

                try
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var toDispose = Env.DecompressionBuffers.GetTemporaryPage(llt, 8 * Constants.Storage.PageSize, out _);

                        decompressedTempPagesToDispose.Add(toDispose);
                    }

                    scratchBuffers = new DirectoryInfo(temp).GetFiles("scratch.*");
                    decompressionBuffers = new DirectoryInfo(temp).GetFiles("decompression.*");

                    totalSize = scratchBuffers.Sum(x => x.Length) + decompressionBuffers.Sum(x => x.Length);

                    Assert.Equal(totalSize, _scratchSpaceMonitor.Size);
                    Assert.Equal(totalSize, Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes);
                    Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);
                }
                finally
                {
                    foreach (var disposable in decompressedTempPagesToDispose)
                    {
                        disposable.Dispose();
                    }
                }
            }

            Env.FlushLogToDataFile();
            Env.ForceSyncDataFile();
            Env.Cleanup();

            Assert.True(_scratchSpaceMonitor.Size < totalSize);
            Assert.True(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes < totalSize);
            Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);

            // Cleanup() has just disposed the old decompression pagers. On Windows they are removed via FILE_FLAG_DELETE_ON_CLOSE,
            // so if any other handle is open on such a file (e.g. AV scanner, indexer) it stays listed until that handle is closed
            totalSize = WaitForSizeOnDiskToMatch(temp, _scratchSpaceMonitor.Size, "scratch.*", "decompression.*");

            Assert.Equal(totalSize, _scratchSpaceMonitor.Size);
            Assert.Equal(totalSize, Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes);
            Assert.Equal(Env.Options.ScratchSpaceUsage.ScratchSpaceInBytes, _scratchSpaceMonitor.Size);

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
