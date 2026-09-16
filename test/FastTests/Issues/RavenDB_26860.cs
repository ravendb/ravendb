using System;
using Raven.Server.Config;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_26860 : NoDisposalNeeded
    {
        public RavenDB_26860(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Voron)]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("2")]
        public void TooSmallIoRingQueueSizeUnderExplicitIoRingWriteModeFailsWithClearError(string queueSize)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.WriteMode), "IoRing");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingQueueSize), queueSize);

            var error = Assert.ThrowsAny<Exception>(() => configuration.Initialize());
            Assert.Contains("IoRingQueueSize", error.Message);
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Voron)]
        [InlineData("IoRing", "-2")]
        [InlineData("IoRing", "-100")]
        [InlineData("Auto", "1")]
        [InlineData("FileIo", "2")]
        public void InvalidIoRingQueueSizeIsRejectedRegardlessOfWriteMode(string writeMode, string queueSize)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.WriteMode), writeMode);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingQueueSize), queueSize);

            var error = Assert.ThrowsAny<Exception>(() => configuration.Initialize());
            Assert.Contains("IoRingQueueSize", error.Message);
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Voron)]
        [InlineData("IoRing", "3")]
        [InlineData("IoRing", "1024")]
        [InlineData("Auto", "-1")]
        [InlineData("Auto", "1024")]
        public void ValidIoRingQueueSizePassesValidation(string writeMode, string queueSize)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.WriteMode), writeMode);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingQueueSize), queueSize);

            configuration.Initialize();

            Assert.Equal(int.Parse(queueSize), configuration.Storage.IoRingQueueSize);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void FailCreateIoRingWithNoSpcErrnoThrowsConfigurationErrorNotDiskFull()
        {
            // 112 = ERROR_DISK_FULL on Windows, 28 = ENOSPC on Posix - both map to the NoSpc special code
            var noSpcErrno = global::Sparrow.Platform.PlatformDetails.RunningOnWindows ? 112 : 28;

            var error = Assert.ThrowsAny<Exception>(() =>
                PalHelper.ThrowLastError(PalFlags.FailCodes.FailCreateIoRing, noSpcErrno, "failed to create io ring"));

            Assert.IsNotType<DiskFullException>(error);
            Assert.Contains("IoRingQueueSize", error.Message);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void GenuineDiskFullStillMapsToDiskFullException()
        {
            var noSpcErrno = global::Sparrow.Platform.PlatformDetails.RunningOnWindows ? 112 : 28;

            Assert.Throws<DiskFullException>(() =>
                PalHelper.ThrowLastError(PalFlags.FailCodes.FailWriteFile, noSpcErrno, "failed to write"));
        }
    }
}
