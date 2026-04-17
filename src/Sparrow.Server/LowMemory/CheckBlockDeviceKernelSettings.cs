using System;
using System.Collections.Generic;
using System.IO;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    public static class CheckBlockDeviceKernelSettings
    {
        private static readonly Logger Log = LoggingSource.Instance.GetLogger("Server", typeof(CheckBlockDeviceKernelSettings).FullName);

        private const string SysBlockPath = "/sys/block/";

        /// <summary>
        /// Returns a list of block devices whose read_ahead_kb value exceeds
        /// <paramref name="thresholdKb"/>, or null if no devices are above the threshold
        /// or if the check cannot be performed (returns null in both cases — the caller
        /// only needs to know whether an alert should be raised).
        /// <para>
        /// A high read_ahead_kb causes the kernel to issue large sequential read-ahead
        /// I/Os on every page fault, which wastes I/O bandwidth and can cause 100% IO wait
        /// under the random-access patterns of a running database.
        /// </para>
        /// Only meaningful on Linux; callers should guard with
        /// <c>PlatformDetails.RunningOnPosix &amp;&amp; !PlatformDetails.RunningOnMacOsx</c>.
        /// </summary>
        public static List<(string DeviceName, Size ReadAheadValue)> GetBlockDevicesWithHighReadAhead(int thresholdKb)
        {
            try
            {
                if (Directory.Exists(SysBlockPath) == false)
                    return null;

                List<(string, Size)> result = null;

                foreach (var blockDir in Directory.GetDirectories(SysBlockPath))
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
                    {
                        result ??= new List<(string, Size)>();
                        result.Add((deviceName, new Size(readAheadKb, SizeUnit.Kilobytes)));
                    }
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
