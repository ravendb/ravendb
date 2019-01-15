// -----------------------------------------------------------------------
//  <copyright file="InitialSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class InitialSize : FastTests.Voron.StorageTest
    {
        public int GetExpectedInitialSize()
        {
            return 64 * 1024;
        }

        [Fact]
        public void WhenInitialFileSizeIsNotSetTheFileSizeForDataFileAndScratchFileShouldBeSetToSystemAllocationGranularity()
        {
            DeleteDataDir();

            var options = StorageEnvironmentOptions.ForPath(DataDir);
            options.InitialFileSize = null;

            using (new StorageEnvironment(options))
            {
                var dataFile = Path.Combine(options.BasePath.FullPath, Constants.DatabaseFilename);
                var scratchFile = Path.Combine(options.TempPath.FullPath, StorageEnvironmentOptions.ScratchBufferName(0));

                Assert.Equal(GetExpectedInitialSize(), new FileInfo(dataFile).Length);
                Assert.Equal(GetExpectedInitialSize(), new FileInfo(scratchFile).Length);
            }
        }

        [Fact]
        public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordingly()
        {
            DeleteDataDir();

            var options = StorageEnvironmentOptions.ForPath(DataDir);
            options.InitialFileSize = GetExpectedInitialSize() * 2;

            using (new StorageEnvironment(options))
            {
                var dataFile = Path.Combine(options.BasePath.FullPath, Constants.DatabaseFilename);
                var scratchFile = Path.Combine(options.TempPath.FullPath, StorageEnvironmentOptions.ScratchBufferName(0));

                Assert.Equal(0, new FileInfo(dataFile).Length % GetExpectedInitialSize());
                Assert.Equal(0, new FileInfo(scratchFile).Length % GetExpectedInitialSize());
            }
        }

        [Fact]
        public void WhenInitialFileSizeIsSetTheFileSizeForDataFileAndScratchFileShouldBeSetAccordinglyAndItWillBeRoundedToTheNearestGranularity()
        {
            DeleteDataDir();

            var options = StorageEnvironmentOptions.ForPath(DataDir);
            options.InitialFileSize = GetExpectedInitialSize() * 2 + 1;

            using (new StorageEnvironment(options))
            {
                var dataFile = Path.Combine(options.BasePath.FullPath, Constants.DatabaseFilename);
                var scratchFile = Path.Combine(options.TempPath.FullPath, StorageEnvironmentOptions.ScratchBufferName(0));

                if (StorageEnvironmentOptions.RunningOnPosix)
                {
                    // on Linux, we use 4K as the allocation granularity
                    Assert.Equal(0, new FileInfo(dataFile).Length % 4096);
                    Assert.Equal(0, new FileInfo(scratchFile).Length % 4096);
                }
                else
                {
                    // on Windows, we use 64K as the allocation granularity
                    Assert.Equal(0, new FileInfo(dataFile).Length % GetExpectedInitialSize());
                    Assert.Equal(0, new FileInfo(scratchFile).Length % GetExpectedInitialSize());
                }
            }
        }

        private void DeleteDataDir()
        {
            if (!string.IsNullOrEmpty(DataDir) && Directory.Exists(DataDir))
                Directory.Delete(DataDir, true);
        }

        public override void Dispose()
        {
            DeleteDataDir();
            base.Dispose();
        }
    }
}
