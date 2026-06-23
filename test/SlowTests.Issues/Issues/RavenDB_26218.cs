using System.IO;
using System.Linq;
using FastTests;
using Sparrow;
using Sparrow.Server.LowMemory;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26218 : RavenTestBase
    {
        public RavenDB_26218(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void GetBlockDevicesWithHighReadAhead_ReportsOnlyDevicesAboveThreshold()
        {
            var sysBlock = NewDataPath(forceCreateDir: true);

            WriteReadAheadKb(sysBlock, "sda", "4096");          // above threshold -> reported
            WriteReadAheadKb(sysBlock, "sdb", "128");           // equal to threshold -> not reported
            WriteReadAheadKb(sysBlock, "nvme0n1", "256");       // above threshold -> reported
            WriteReadAheadKb(sysBlock, "loop0", "8192");        // loop device -> skipped despite being high
            WriteReadAheadKb(sysBlock, "dm-0", "not-a-number"); // malformed value -> skipped
            Directory.CreateDirectory(Path.Combine(sysBlock, "sdc")); // no queue/read_ahead_kb -> skipped

            var result = CheckBlockDeviceKernelSettings.GetBlockDevicesWithHighReadAhead(thresholdKb: 128, sysBlockPath: sysBlock);

            Assert.NotNull(result);
            var byName = result.ToDictionary(x => x.DeviceName, x => x.ReadAheadValue);

            Assert.Equal(2, byName.Count);
            Assert.Equal(4096, byName["sda"].GetValue(SizeUnit.Kilobytes));
            Assert.Equal(256, byName["nvme0n1"].GetValue(SizeUnit.Kilobytes));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void GetBlockDevicesWithHighReadAhead_ReturnsEmpty_WhenNothingAboveThreshold()
        {
            var sysBlock = NewDataPath(forceCreateDir: true);
            WriteReadAheadKb(sysBlock, "sda", "128");
            WriteReadAheadKb(sysBlock, "sdb", "64");

            var result = CheckBlockDeviceKernelSettings.GetBlockDevicesWithHighReadAhead(thresholdKb: 128, sysBlockPath: sysBlock);
            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void GetBlockDevicesWithHighReadAhead_ReturnsNull_WhenSysBlockPathMissing()
        {
            var missing = Path.Combine(NewDataPath(forceCreateDir: true), "does-not-exist");

            Assert.Null(CheckBlockDeviceKernelSettings.GetBlockDevicesWithHighReadAhead(thresholdKb: 128, sysBlockPath: missing));
        }

        private static void WriteReadAheadKb(string sysBlockRoot, string device, string content)
        {
            var queueDir = Path.Combine(sysBlockRoot, device, "queue");
            Directory.CreateDirectory(queueDir);
            File.WriteAllText(Path.Combine(queueDir, "read_ahead_kb"), content);
        }
    }
}
