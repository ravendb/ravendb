using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;
using Sparrow.Platform;

namespace Sparrow.Server.Utils
{
    /// <summary>
    /// Reads the time-weighted queue depth of one block device (the "aqu-sz" number from iostat).
    /// One instance holds one open handle for the lifetime of the process; each Read costs one
    /// small pread (Linux) or one counter sample (Windows). Returns null from TryCreate when the
    /// platform gives no such data (macOS, containers without sysfs, broken perf counters) - the
    /// caller must treat that as "no signal", never as "queue is empty".
    /// </summary>
    public abstract class DeviceQueueDepthReader : IDisposable
    {
        /// <returns>the mean count of I/O operations in flight since the previous call</returns>
        public abstract double Read();

        public abstract void Dispose();

        public static DeviceQueueDepthReader TryCreate(string pathOnDevice, ulong deviceId)
        {
            try
            {
                if (PlatformDetails.RunningOnLinux)
                    return LinuxSysfsReader.TryCreate(deviceId);
                if (PlatformDetails.RunningOnWindows)
                    return WindowsCounterReader.TryCreate(pathOnDevice);
                return null; // macOS and others: no signal, by design
            }
            catch
            {
                return null;
            }
        }

        private sealed class LinuxSysfsReader : DeviceQueueDepthReader
        {
            private readonly SafeFileHandle _handle;
            private readonly Stopwatch _clock = Stopwatch.StartNew();
            private long _previousElapsedMs;
            private long _previousQueueMs;

            private LinuxSysfsReader(SafeFileHandle handle)
            {
                _handle = handle;
                _previousElapsedMs = _clock.ElapsedMilliseconds;
                _previousQueueMs = ReadQueueMs();
            }

            public static LinuxSysfsReader TryCreate(ulong deviceId)
            {
                // st_dev encodes major:minor, see sysmacros.h
                var major = ((deviceId & 0x00000000000fff00UL) >> 8) | ((deviceId & 0xfffff00000000000UL) >> 32);
                var minor = (deviceId & 0x00000000000000ffUL) | ((deviceId & 0x00000ffffff00000UL) >> 12);

                var path = $"/sys/dev/block/{major}:{minor}/stat";
                if (File.Exists(path) == false)
                    return null;

                var handle = File.OpenHandle(path);
                var reader = new LinuxSysfsReader(handle);
                if (reader._previousQueueMs < 0)
                {
                    reader.Dispose();
                    return null; // the stat line has no field 11 (very old kernel or partition-only stats)
                }

                return reader;
            }

            public override double Read()
            {
                var queueMs = ReadQueueMs();
                var nowMs = _clock.ElapsedMilliseconds;

                var elapsedMs = nowMs - _previousElapsedMs;
                var value = queueMs < 0 || elapsedMs <= 0
                    ? 0
                    : (queueMs - _previousQueueMs) / (double)elapsedMs;

                _previousElapsedMs = nowMs;
                if (queueMs >= 0)
                    _previousQueueMs = queueMs;
                return value;
            }

            [SkipLocalsInit]
            private long ReadQueueMs()
            {
                Span<byte> buffer = stackalloc byte[512];
                ReadOnlySpan<byte> span = buffer[..RandomAccess.Read(_handle, buffer, 0)];

                // Skip the first 10 fields
                for (var i = 0; i < 10; i++)
                {
                    span = span.TrimStart((byte)' ');
                    var end = span.IndexOf((byte)' ');
                    if (end < 0) return -1;
                    span = span[end..];
                }

                // Parse the 11th field (Utf8Parser ignores trailing spaces/newlines automatically)
                span = span.TrimStart((byte)' ');
                return Utf8Parser.TryParse(span, out long value, out _) ? value : -1;
            }

            public override void Dispose()
            {
                _handle.Dispose();
            }
        }

        [SupportedOSPlatform("windows")]
        private sealed class WindowsCounterReader : DeviceQueueDepthReader
        {
            private readonly PerformanceCounter _counter;

            private WindowsCounterReader(PerformanceCounter counter)
            {
                _counter = counter;
            }

            public static WindowsCounterReader TryCreate(string pathOnDevice)
            {
                var drive = DiskUtils.WindowsGetDriveName(pathOnDevice, out _);
                var category = new PerformanceCounterCategory("LogicalDisk");
                foreach (var name in category.GetInstanceNames())
                {
                    // GetInstanceNames returns "C:" while WindowsGetDriveName returns "C:\"
                    if (drive.StartsWith(name, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    var counter = new PerformanceCounter("LogicalDisk", "Avg. Disk Queue Length", name, readOnly: true);
                    counter.NextValue(); // the first sample only sets the baseline
                    return new WindowsCounterReader(counter);
                }

                return null;
            }

            public override double Read()
            {
                lock(_counter)
                {
                    return _counter.NextValue();
                }
            }

            public override void Dispose()
            {
                _counter.Dispose();
            }
        }
    }
}
