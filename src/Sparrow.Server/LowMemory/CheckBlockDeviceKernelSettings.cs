using System;
using System.Collections.Generic;
using System.IO;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    public static class CheckBlockDeviceKernelSettings
    {
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(CheckBlockDeviceKernelSettings));

        private const string SysBlockPath = "/sys/block/";

        /// <summary>
        /// Returns the block devices whose read_ahead_kb exceeds <paramref name="thresholdKb"/>,
        /// or an empty list if the check ran but found none. Returns <c>null</c> only when the check
        /// could not be performed (sysfs tree missing, or an error reading it); callers must treat
        /// null as "unknown", never as "all clear", so a failed check cannot clear a real warning.
        /// <para>
        /// A high read_ahead_kb causes the kernel to issue large sequential read-ahead
        /// I/Os on every page fault, which wastes I/O bandwidth and can cause 100% IO wait
        /// under the random-access patterns of a running database.
        /// </para>
        /// Only meaningful on Linux; callers should guard with
        /// <c>PlatformDetails.RunningOnPosix &amp;&amp; !PlatformDetails.RunningOnMacOsx</c>.
        /// </summary>
        /// <param name="thresholdKb">Devices with read_ahead_kb above this are returned.</param>
        /// <param name="sysBlockPath">Root of the sysfs block-device tree. Defaults to /sys/block/; overridable for testing.</param>
        public static List<(string DeviceName, Size ReadAheadValue)> GetBlockDevicesWithHighReadAhead(int thresholdKb, string sysBlockPath = SysBlockPath)
        {
            try
            {
                if (Directory.Exists(sysBlockPath) == false)
                    return null;

                var result = new List<(string, Size)>();

                foreach (var blockDir in Directory.GetDirectories(sysBlockPath))
                {
                    var deviceName = Path.GetFileName(blockDir);

                    // loop devices are virtual and always report a high read_ahead_kb — skip them
                    if (deviceName.StartsWith("loop", StringComparison.Ordinal))
                        continue;

                    var readAheadFile = Path.Combine(blockDir, "queue", "read_ahead_kb");
                    if (File.Exists(readAheadFile) == false)
                        continue;

                    var readAheadKb = KernelVirtualFileSystemUtils.ReadNumberFromFile(readAheadFile);
                    if (readAheadKb == long.MaxValue) // ReadNumberFromFile returns long.MaxValue on error
                        continue;

                    if (readAheadKb > thresholdKb)
                        result.Add((deviceName, new Size(readAheadKb, SizeUnit.Kilobytes)));
                }

                return result;
            }
            catch (Exception ex)
            {
                if (Log.IsInfoEnabled)
                    Log.Info("Error while trying to determine read_ahead_kb values for block devices, ignoring this check", ex);
                return null;
            }
        }
    }
}
