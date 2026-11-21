using System;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Mono.Unix.Native;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;

namespace Sparrow.Server.Utils;

/// <summary>The I/O of one block device in the last sample interval.</summary>
public sealed record DeviceIoStats
{
    public double ReadsPerSecond { get; init; }
    public double WritesPerSecond { get; init; }
    public double ReadBytesPerSecond { get; init; }
    public double WriteBytesPerSecond { get; init; }

    /// <summary>The time-weighted mean count of I/O operations in flight (iostat "aqu-sz").</summary>
    public double QueueDepth { get; init; }

    /// <summary>The mean time of a read in the interval, 0 when there was no read.</summary>
    public double ReadLatencyMs { get; init; }

    /// <summary>The mean time of a write in the interval, 0 when there was no write.</summary>
    public double WriteLatencyMs { get; init; }

    public double IntervalMs { get; init; }
}

/// <summary>
/// The one reader of a block device's I/O counters (sysfs on Linux, LogicalDisk performance counters on Windows), sampled
/// once per <see cref="SampleIntervalMs"/>. Null from <see cref="ForDevice"/> means "no signal", never "the device is idle".
/// </summary>
public sealed class DeviceIoSampler
{
    public const int SampleIntervalMs = 1000;
    public const int RetryIntervalMs = 60_000;

    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(DeviceIoSampler));
    private static readonly ConcurrentDictionary<ulong, Lazy<DeviceIoSampler>> ByDevice = new();
    private static readonly ConcurrentDictionary<string, ulong> DeviceByPath = new();
    internal static readonly ConcurrentDictionary<ulong, long> RetryAt = new(); // after a failure to open the counters

    /// <summary>For tests: opens a device's counters first; null opens the real ones.</summary>
    internal static Func<ulong, string, DeviceCounters> OpenCountersForTesting;
    private static readonly Lazy<Timer> SampleTimer = new(() => new Timer(_ => SampleAll(), null, SampleIntervalMs, SampleIntervalMs));
    private static int _sampling;

    private readonly DeviceCounters _counters;
    private DeviceIoStats _current;

    internal DeviceIoSampler(ulong deviceId, string pathOnDevice, DeviceCounters counters)
    {
        DeviceId = deviceId;
        PathOnDevice = pathOnDevice;
        _counters = counters;
    }

    public ulong DeviceId { get; }

    public string PathOnDevice { get; }

    /// <summary>The last interval, null before the second reading of the counters.</summary>
    public DeviceIoStats Current => Volatile.Read(ref _current);

    /// <summary>Runs on the timer thread after each sample; a handler must not block.</summary>
    public event Action<DeviceIoStats> Sampled;

    public static IEnumerable<DeviceIoSampler> All
    {
        get
        {
            foreach (var sampler in ByDevice.Values)
            {
                if (sampler.IsValueCreated == false)
                    continue;
                DeviceIoSampler value;
                try
                {
                    value = sampler.Value;
                }
                catch
                {
                    continue; // a failed open, about to be removed by ForDevice
                }
                if (value != null)
                    yield return value;
            }
        }
    }

    /// <param name="deviceId">st_dev on Linux, the volume serial number on Windows (as rvn_pager_get_device_id)</param>
    /// <param name="pathOnDevice">a file or directory on the device</param>
    public static DeviceIoSampler ForDevice(ulong deviceId, string pathOnDevice)
    {
        if (ByDevice.TryGetValue(deviceId, out var existing) == false)
        {
            if (RetryAt.TryGetValue(deviceId, out var retryAt) && Environment.TickCount64 < retryAt)
                return null;
            existing = ByDevice.GetOrAdd(deviceId, static (id, path) => new Lazy<DeviceIoSampler>(() => Create(id, path)), pathOnDevice);
        }

        try
        {
            return existing.Value; // null: the device has no counters, which does not change
        }
        catch (Exception e)
        {
            // opening the counters failed (out of descriptors, performance counters not ready): forget it and try again later
            ByDevice.TryRemove(new KeyValuePair<ulong, Lazy<DeviceIoSampler>>(deviceId, existing));
            RetryAt[deviceId] = Environment.TickCount64 + RetryIntervalMs;
            if (Logger.IsInfoEnabled)
                Logger.Info($"Cannot read the I/O counters of the device that holds '{pathOnDevice}', trying again in {RetryIntervalMs / 1000} seconds", e);
            return null;
        }
    }

    public static DeviceIoSampler ForPath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return null;

        try
        {
            if (DeviceByPath.TryGetValue(path, out var deviceId) == false)
            {
                if (TryGetDeviceId(path, out deviceId) == false)
                    return null; // not cached: the path may not exist yet
                DeviceByPath.TryAdd(path, deviceId);
            }

            return ForDevice(deviceId, path);
        }
        catch (Exception e)
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug($"Could not find the device of '{path}'", e);
            return null;
        }
    }

    internal void Sample(long timestamp)
    {
        if (_counters.TrySample(timestamp, out var stats) == false)
            return;

        Volatile.Write(ref _current, stats);
        Sampled?.Invoke(stats);
    }

    private static DeviceIoSampler Create(ulong deviceId, string pathOnDevice)
    {
        var counters = OpenCountersForTesting?.Invoke(deviceId, pathOnDevice) ?? DeviceCounters.TryCreate(deviceId, pathOnDevice);
        if (counters == null)
            return null;

        _ = SampleTimer.Value;
        return new DeviceIoSampler(deviceId, pathOnDevice, counters);
    }

    private static void SampleAll()
    {
        if (Interlocked.Exchange(ref _sampling, 1) == 1)
            return; // the previous tick still runs

        try
        {
            var timestamp = Stopwatch.GetTimestamp();
            foreach (var sampler in All)
            {
                try
                {
                    sampler.Sample(timestamp);
                }
                catch (Exception e)
                {
                    // a failed read is a lost sample, nothing more
                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Could not sample the I/O counters of the device that holds '{sampler.PathOnDevice}'", e);
                }
            }
        }
        finally
        {
            Volatile.Write(ref _sampling, 0);
        }
    }

    private static bool TryGetDeviceId(string path, out ulong deviceId)
    {
        deviceId = 0;
        if (PlatformDetails.RunningOnLinux)
            return TryGetLinuxDeviceId(path, out deviceId);

        if (PlatformDetails.RunningOnWindows)
            return TryGetVolumeSerialNumber(path, out deviceId);

        return false;
    }

    // Mono.Unix types only here: compiling a method with a Stat local loads Mono.Posix.NETStandard, which is not deployed on
    // every Windows runtime (no win-arm64 build), so a Windows caller must not reach it
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryGetLinuxDeviceId(string path, out ulong deviceId)
    {
        deviceId = 0;
        if (Syscall.stat(path, out var stat) != 0)
            return false;
        deviceId = (stat.st_mode & FilePermissions.S_IFMT) == FilePermissions.S_IFBLK ? stat.st_rdev : stat.st_dev;
        return true;
    }

    private static unsafe bool TryGetVolumeSerialNumber(string path, out ulong serialNumber)
    {
        serialNumber = 0;
        var root = new char[path.Length + 1024]; // the volume root, usually a prefix of the path plus a trailing backslash
        fixed (char* rootPtr = root)
        {
            if (GetVolumePathNameW(path, rootPtr, (uint)root.Length) == false)
                return false;
            if (GetVolumeInformationW(rootPtr, null, 0, out var serial, out _, out _, null, 0) == false)
                return false;
            serialNumber = serial;
        }

        return true;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern unsafe bool GetVolumePathNameW(string fileName, char* volumePathName, uint bufferLength);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern unsafe bool GetVolumeInformationW(char* rootPathName, char* volumeNameBuffer, uint volumeNameSize, out uint volumeSerialNumber,
        out uint maximumComponentLength, out uint fileSystemFlags, char* fileSystemNameBuffer, uint fileSystemNameSize);
}

/// <summary>A device's counters: each sample covers the time since the previous one; the first only sets the baseline.</summary>
internal abstract class DeviceCounters : IDisposable
{
    /// <param name="timestamp">Stopwatch.GetTimestamp() at the sample</param>
    public abstract bool TrySample(long timestamp, out DeviceIoStats stats);

    public virtual void Dispose()
    {
    }

    public static DeviceCounters TryCreate(ulong deviceId, string pathOnDevice)
    {
        if (PlatformDetails.RunningOnLinux)
            return LinuxSysfsCounters.TryCreate(deviceId);
        if (PlatformDetails.RunningOnWindows)
            return WindowsPerformanceCounters.TryCreate(pathOnDevice);
        return null; // macOS and others: no signal, by design
    }

    internal sealed class LinuxSysfsCounters : DeviceCounters
    {
        // https://www.kernel.org/doc/Documentation/block/stat.txt
        private const int Reads = 0, ReadSectors = 2, ReadTicksMs = 3, Writes = 4, WriteSectors = 6, WriteTicksMs = 7, InFlight = 8, WeightedTicksMs = 10;
        private const int FieldsUsed = 11;
        private const int SectorSize = 512;

        private readonly SafeFileHandle _handle;
        private readonly long[] _previous = new long[FieldsUsed];
        private long _previousTimestamp;

        private LinuxSysfsCounters(SafeFileHandle handle)
        {
            _handle = handle;
        }

        public static LinuxSysfsCounters TryCreate(ulong deviceId)
        {
            // st_dev encodes major:minor, see sysmacros.h
            var major = ((deviceId & 0x00000000000fff00UL) >> 8) | ((deviceId & 0xfffff00000000000UL) >> 32);
            var minor = (deviceId & 0x00000000000000ffUL) | ((deviceId & 0x00000ffffff00000UL) >> 12);
            return TryCreate($"/sys/dev/block/{major}:{minor}/stat");
        }

        internal static LinuxSysfsCounters TryCreate(string statPath)
        {
            if (File.Exists(statPath) == false)
                return null; // overlayfs, tmpfs and other devices without a block device

            var counters = new LinuxSysfsCounters(File.OpenHandle(statPath));
            if (counters.TryRead(stackalloc long[FieldsUsed]) == false)
            {
                counters._handle.Dispose();
                return null; // the stat line has no field 11 (very old kernel)
            }

            return counters;
        }

        public override bool TrySample(long timestamp, out DeviceIoStats stats)
        {
            stats = null;
            Span<long> current = stackalloc long[FieldsUsed];
            if (TryRead(current) == false)
                return false;

            var hasBaseline = _previousTimestamp != 0;
            var seconds = (timestamp - _previousTimestamp) / (double)Stopwatch.Frequency;
            Span<long> delta = stackalloc long[FieldsUsed];
            for (var i = 0; i < FieldsUsed; i++)
            {
                delta[i] = current[i] - _previous[i];
                if (delta[i] < 0 && i != InFlight)
                    hasBaseline = false; // the counters were reset (the device was re-added); in flight is a gauge, not a counter
            }

            current.CopyTo(_previous);
            _previousTimestamp = timestamp;
            if (hasBaseline == false || seconds <= 0)
                return false;

            stats = new DeviceIoStats
            {
                ReadsPerSecond = delta[Reads] / seconds,
                WritesPerSecond = delta[Writes] / seconds,
                ReadBytesPerSecond = delta[ReadSectors] * SectorSize / seconds,
                WriteBytesPerSecond = delta[WriteSectors] * SectorSize / seconds,
                QueueDepth = delta[WeightedTicksMs] / (seconds * 1000),
                ReadLatencyMs = delta[Reads] == 0 ? 0 : delta[ReadTicksMs] / (double)delta[Reads],
                WriteLatencyMs = delta[Writes] == 0 ? 0 : delta[WriteTicksMs] / (double)delta[Writes],
                IntervalMs = seconds * 1000
            };
            return true;
        }

        public override void Dispose() => _handle.Dispose();

        [SkipLocalsInit]
        private bool TryRead(Span<long> values)
        {
            Span<byte> buffer = stackalloc byte[512];
            ReadOnlySpan<byte> line = buffer[..RandomAccess.Read(_handle, buffer, 0)];
            for (var i = 0; i < values.Length; i++)
            {
                line = line.TrimStart((byte)' ');
                if (Utf8Parser.TryParse(line, out long value, out var consumed) == false)
                    return false;
                values[i] = value;
                line = line[consumed..];
            }

            return true;
        }
    }

    [SupportedOSPlatform("windows")]
    private sealed class WindowsPerformanceCounters : DeviceCounters
    {
        private const string Category = "LogicalDisk";

        private readonly PerformanceCounter _reads;
        private readonly PerformanceCounter _writes;
        private readonly PerformanceCounter _readBytes;
        private readonly PerformanceCounter _writeBytes;
        private readonly PerformanceCounter _queueDepth;
        private readonly PerformanceCounter _readLatency;
        private readonly PerformanceCounter _writeLatency;
        private long _previousTimestamp;

        private WindowsPerformanceCounters(string instance)
        {
            _reads = new PerformanceCounter(Category, "Disk Reads/sec", instance, readOnly: true);
            _writes = new PerformanceCounter(Category, "Disk Writes/sec", instance, readOnly: true);
            _readBytes = new PerformanceCounter(Category, "Disk Read Bytes/sec", instance, readOnly: true);
            _writeBytes = new PerformanceCounter(Category, "Disk Write Bytes/sec", instance, readOnly: true);
            _queueDepth = new PerformanceCounter(Category, "Avg. Disk Queue Length", instance, readOnly: true);
            _readLatency = new PerformanceCounter(Category, "Avg. Disk sec/Read", instance, readOnly: true);
            _writeLatency = new PerformanceCounter(Category, "Avg. Disk sec/Write", instance, readOnly: true);
        }

        public static WindowsPerformanceCounters TryCreate(string pathOnDevice)
        {
            var drive = DiskUtils.WindowsGetDriveName(pathOnDevice, out _);
            foreach (var name in new PerformanceCounterCategory(Category).GetInstanceNames())
            {
                // GetInstanceNames returns "C:" while WindowsGetDriveName returns "C:\"
                if (drive.StartsWith(name, StringComparison.OrdinalIgnoreCase))
                    return new WindowsPerformanceCounters(name);
            }

            return null;
        }

        public override bool TrySample(long timestamp, out DeviceIoStats stats)
        {
            // each counter computes its value from its own previous sample
            stats = new DeviceIoStats
            {
                ReadsPerSecond = _reads.NextValue(),
                WritesPerSecond = _writes.NextValue(),
                ReadBytesPerSecond = _readBytes.NextValue(),
                WriteBytesPerSecond = _writeBytes.NextValue(),
                QueueDepth = _queueDepth.NextValue(),
                ReadLatencyMs = _readLatency.NextValue() * 1000,
                WriteLatencyMs = _writeLatency.NextValue() * 1000,
                IntervalMs = (timestamp - _previousTimestamp) * 1000.0 / Stopwatch.Frequency
            };

            var hasBaseline = _previousTimestamp != 0;
            _previousTimestamp = timestamp;
            if (hasBaseline)
                return true;

            stats = null;
            return false;
        }

        public override void Dispose()
        {
            _reads.Dispose();
            _writes.Dispose();
            _readBytes.Dispose();
            _writeBytes.Dispose();
            _queueDepth.Dispose();
            _readLatency.Dispose();
            _writeLatency.Dispose();
        }
    }
}
