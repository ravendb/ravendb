using System;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron.Logging;

namespace Voron.Impl.Journal
{
    /// <summary>
    /// Per physical device write policy / budget goes here. All environments on the same disk
    /// share the same limits and budgets.
    ///
    /// Decisions owned here:
    ///
    /// * Writeback mode:
    ///     - Trickle: each flush starts writeback of its dirty ranges (async) and the sync is a plain fdatasync; best while the device has headroom. 
    ///     - Drain:   flushes do not start writeback; the sync pushes the dirty ranges in bounded blocks before the fdatasync(), limiting the I/O we 
    ///                 generate benefitting from dead-write merging. 
    ///        
    ///     Using device queue depth (the iostat "aqu-sz" number, sampled once a second).
    ///
    /// * Device classification:
    ///     - Fast (nvme-like: limits so high we never hit them)
    ///     - Budgeted (gp3-like: metered bandwidth/IOPS we do hit)
    /// 
    ///   Using: the journal write latency and size observed across every environment on the device. 
    ///   Feeds the codec choice and the compression threshold in each environment's WriteFlowPolicy.
    ///
    /// * Journal zeroing / pool prewarming - prepaying the filesystem extent-conversion cost
    ///   only pays on a fast local device; on a budgeted volume the fill competes with every
    ///   journal on the disk (measured 8-17% of throughput on gp3 under load).
    /// </summary>
    public sealed class DeviceWriteBudget
    {
        private static readonly ConcurrentDictionary<ulong, DeviceWriteBudget> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<DeviceWriteBudget>();

        public static DeviceWriteBudget GetForDevice(ulong deviceId, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold, long classifyAboveLatencyTicks)
        {
            if (DevicesById.TryGetValue(deviceId, out var existing))
                return existing;

            return Unlikely();

            DeviceWriteBudget Unlikely()
            {
                var reader = DeviceQueueDepthReader.TryCreate(pathOnDevice, deviceId);
                var candidate = new DeviceWriteBudget(reader, pathOnDevice, syncCostThresholdTicks, queueDepthThreshold, classifyAboveLatencyTicks, deviceId);
                var winner = DevicesById.GetOrAdd(deviceId, candidate);
                if (ReferenceEquals(winner, candidate) == false)
                    reader?.Dispose(); // lost the race - don't leak the device handle
                return winner;
            }
        }

        public static DeviceWriteBudget CreateUnshared(StorageEnvironmentOptions opts) =>
            new(queueReader: null, pathOnDevice: "(unshared)", opts.SyncWritebackBarrierCostThresholdTicks, opts.SyncWritebackDrainQueueDepthThreshold, opts.PipelineJournalWritesAboveLatencyInTicks);

        private const long SampleIntervalMs = 1_000;
        private const long ExitQuietMs = 30_000;
        internal const int RecentWriteActivityWindowMs = 3;
        // in Stopwatch.GetTimestamp units (Stopwatch.Frequency per second - NOT TimeSpan ticks)
        private static readonly long RecentWriteActivityWindowTimestampTicks = RecentWriteActivityWindowMs * Stopwatch.Frequency / 1000;
        private readonly DeviceQueueDepthReader _queueReader; // null = no queue signal on this platform
        private readonly string _pathOnDevice;
        private readonly long _syncThresholdTicks;
        private readonly double _enterQueueDepth;
        private readonly double _exitQueueDepth;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private double _activeQueueThreshold; // the enter value while trickling, the exit value while draining
        
        private Sparrow.Server.Utils.SimpleEwma<long> _syncCostTicksEwma = new(smoothing: 4, 
            validityMs: 60_000); // sync happens rarely, so we give it plenty of time to expire any measurements
        
        private Sparrow.Server.Utils.SimpleEwma<double> _queueDepth = new(smoothing: 4);
        private long _lastSampleMs;
        private long _lastBusyMs;
        private bool _draining;
        private long _lastSwitchUtcTicks;

        internal DeviceWriteBudget(DeviceQueueDepthReader queueReader, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold, long classifyAboveLatencyTicks,
            ulong deviceId = 0)
        {
            DeviceId = deviceId;
            _queueReader = queueReader;
            _pathOnDevice = pathOnDevice;
            _syncThresholdTicks = syncCostThresholdTicks;
            _enterQueueDepth = queueDepthThreshold;
            _exitQueueDepth = queueDepthThreshold * 0.6; // leave only well below the entry point
            _activeQueueThreshold = _enterQueueDepth;
            _classifyAboveLatencyTicks = classifyAboveLatencyTicks;
        }

        public enum DeviceClass
        {
            Unknown, // no evidence yet, go for safe defaults
            Fast,    // example: nvme - very high limits, or we never hit them
            Budgeted // example: gp3 - both bandwidth & IOPS limits that we hit
        }

        // journal write telemetry across EVERY environment on this device, intentionally long-lived because it shows disk perf
        // if the disk perf change (burstable, load, etc), we'll update the status with ~8 measurements anyway
        private Sparrow.Server.Utils.SimpleEwma<long> _journalWriteLatencyTicks = new(smoothing: 8, validityMs: Sparrow.Server.Utils.SimpleEwma.NeverExpires);
        private Sparrow.Server.Utils.SimpleEwma<long> _journalWriteSizeBytes = new(smoothing: 8, validityMs: Sparrow.Server.Utils.SimpleEwma.NeverExpires);
        private long _lastJournalWriteActivityTimestamp;
        private readonly long _classifyAboveLatencyTicks;

        public void RecordJournalWrite(long latencyTicks, long sizeInBytes, long time)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, time);
            _journalWriteLatencyTicks.Update(latencyTicks);
            _journalWriteSizeBytes.Update(sizeInBytes);
        }

        public void RecordJournalWriteActivity(long time)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, time);
        }

        public bool JournalWriteRecentlyActive =>
            Stopwatch.GetTimestamp() - Volatile.Read(ref _lastJournalWriteActivityTimestamp) < RecentWriteActivityWindowTimestampTicks;

        public enum ClassReason
        {
            NoWrites,       // no journal write measured yet
            VeryLowLatency, // below threshold / 8: fast whatever the write size
            WritesTooSmall, // average write below SmallWriteBytes: a small write can be fast on a slow device
            LowLatency,     // large writes below threshold / 2
            HighLatency     // large writes at or above threshold / 2
        }

        public const long SmallWriteBytes = 256 * Global.Constants.Size.Kilobyte;

        public DeviceClass MeasuredDeviceClass => Classify(out _);

        public DeviceClass Classify(out ClassReason reason)
        {
            var ewma = _journalWriteLatencyTicks.Current;
            var threshold = _classifyAboveLatencyTicks;
            if (ewma == 0 || threshold == 0)
            {
                reason = ClassReason.NoWrites;
                return DeviceClass.Unknown;
            }

            // This is really fast, consider it a fast disk regardless of the size threshold
            if (ewma < threshold / 8)
            {
                reason = ClassReason.VeryLowLatency;
                return DeviceClass.Fast;
            }

            // small writes can be fast on a slow device, so we can't estimate from small writes only
            if (_journalWriteSizeBytes.Current < SmallWriteBytes)
            {
                reason = ClassReason.WritesTooSmall;
                return DeviceClass.Unknown;
            }

            if (ewma < threshold / 2)
            {
                reason = ClassReason.LowLatency;
                return DeviceClass.Fast;
            }

            reason = ClassReason.HighLatency;
            return DeviceClass.Budgeted;
        }

        // read-only view for diagnostics, none of these are used by the write path

        public ulong DeviceId { get; }

        public string PathOnDevice => _pathOnDevice;

        public TimeSpan JournalWriteLatency => TimeSpan.FromTicks(_journalWriteLatencyTicks.Current);

        public long JournalWriteSizeInBytes => _journalWriteSizeBytes.Current;

        public TimeSpan TimeSinceLastJournalWrite
        {
            get
            {
                long last = Volatile.Read(ref _lastJournalWriteActivityTimestamp);
                return last == 0 ? TimeSpan.MaxValue : Stopwatch.GetElapsedTime(last);
            }
        }

        // null when the platform gives no queue depth signal
        public double? QueueDepth => _queueReader == null ? null : _queueDepth.Current;

        public TimeSpan SyncCost => TimeSpan.FromTicks(_syncCostTicksEwma.Current);

        // _draining only changes inside ShouldDrain() (flush / sync), so on a device that went quiet the flag is stale.
        // Mirror the exit rule instead of reporting the stale flag.
        public bool IsDraining => _draining && _clock.ElapsedMilliseconds - Volatile.Read(ref _lastBusyMs) < ExitQuietMs;

        public DateTime? LastModeSwitch
        {
            get
            {
                long ticks = Volatile.Read(ref _lastSwitchUtcTicks);
                return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
            }
        }

        public TimeSpan SyncCostThreshold => TimeSpan.FromTicks(_syncThresholdTicks);

        public double EnterDrainQueueDepth => _enterQueueDepth;

        public double ExitDrainQueueDepth => _exitQueueDepth;

        public TimeSpan ClassifyAboveLatency => TimeSpan.FromTicks(_classifyAboveLatencyTicks);

        public bool IsMeasuredFastDevice => MeasuredDeviceClass == DeviceClass.Fast;

        // fallocated file still pay for extent allocation, visible on NVMe devices (60% of write cost), pre-zero fill fixes that.
        // slow devices (gp3) have a bandwidth budget, zero-fill competes with journal writes, so we need to skip that there.
        public bool ShouldPrepareZeroedJournalsInBackground => IsMeasuredFastDevice;

        private const int MaxJournalZeroingStallMs = 500;

        // callback from zero fill PAL, let it know when it should pace itself to avoid contentions with journal
        public int NextJournalZeroingStepMs(bool journalWriteActive, int stalledSoFarMs)
        {
            if (IsMeasuredFastDevice == false)
                return -1;

            if (journalWriteActive == false && JournalWriteRecentlyActive == false)
                return 0; // write the next chunk immediately

            if (stalledSoFarMs >= MaxJournalZeroingStallMs)
                return -1; // no sign of going quiet - abort

            return RecentWriteActivityWindowMs;
        }

        public void RecordSyncCost(long ticks) => _syncCostTicksEwma.Update(ticks);

        public bool ShouldDrain()
        {
            var (nowMs, queueDepth) = SampleQueue();
            bool wasDraining = _draining;
            bool draining = UpdateDrainMode(nowMs, queueDepth);
            if (Log.IsInfoEnabled && (draining != wasDraining || Volatile.Read(ref _suppressedModeSwitchLogs) > 0))
                LogModeSwitch(nowMs, queueDepth, switched: draining != wasDraining);
            return draining;
        }

        private bool UpdateDrainMode(long nowMs, double queueDepth)
        {
            // either high device queue depth, or high sync cost (too much I/O for the device to keep up)
            if (queueDepth > _activeQueueThreshold || _syncCostTicksEwma.Current > _syncThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThreshold = _exitQueueDepth;
                    Volatile.Write(ref _lastSwitchUtcTicks, DateTime.UtcNow.Ticks);
                }
                return true;
            }

            if (_draining is false)
                return false;

            // we require a quiet period before we exit drain mode, to avoid thrashing back and forth
            if (nowMs - _lastBusyMs < ExitQuietMs)
                return true;

            _draining = false;
            _activeQueueThreshold = _enterQueueDepth;
            Volatile.Write(ref _lastSwitchUtcTicks, DateTime.UtcNow.Ticks);
            return false;
        }

        private const long ModeSwitchLogIntervalMs = 60_000;
        private long _lastModeSwitchLogMs = -ModeSwitchLogIntervalMs;
        private int _suppressedModeSwitchLogs;

        // a device flapping at the edge of congestion must not flood the log: one line per minute. Switches in between are counted
        // and reported by the first call after the minute, so the last line always shows the mode the device is in.
        private void LogModeSwitch(long nowMs, double queueDepth, bool switched)
        {
            if (nowMs - Volatile.Read(ref _lastModeSwitchLogMs) < ModeSwitchLogIntervalMs)
            {
                if (switched)
                    Interlocked.Increment(ref _suppressedModeSwitchLogs);
                return;
            }

            Volatile.Write(ref _lastModeSwitchLogMs, nowMs);
            int suppressed = Interlocked.Exchange(ref _suppressedModeSwitchLogs, 0);
            string notLogged = suppressed > 0 ? $" {suppressed:#,#} mode switch(es) since the previous line were not logged." : string.Empty;
            string since = switched ? string.Empty : $" In this mode since {LastModeSwitch:HH:mm:ss} UTC.";

            if (_draining)
            {
                Log.Info($"The device that holds '{_pathOnDevice}' is congested ({DescribeInputs(queueDepth)}). Every environment on this device " +
                         "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                         $"in paced blocks before the fdatasync.{notLogged}{since}");
            }
            else
            {
                Log.Info($"The device that holds '{_pathOnDevice}' is quiet again ({DescribeInputs(queueDepth)}). Every environment " +
                         "on this device returns to trickle mode: flushes start writeback of their dirty ranges, and syncs " +
                         $"use a plain fdatasync.{notLogged}{since}");
            }
        }

        private string DescribeInputs(double queueDepth)
        {
            string queue = _queueReader == null ? "no queue depth on this platform" : $"queue depth {queueDepth:0.0}, drain above {_enterQueueDepth:0.#} / leave below {_exitQueueDepth:0.#}";
            return $"{queue}, sync cost {TimeSpan.FromTicks(_syncCostTicksEwma.Current).TotalMilliseconds:#,#0} ms (drain above {TimeSpan.FromTicks(_syncThresholdTicks).TotalMilliseconds:#,#0} ms), " +
                   $"journal writes {TimeSpan.FromTicks(_journalWriteLatencyTicks.Current).TotalMilliseconds:#,#0.#} ms for {_journalWriteSizeBytes.Current / Global.Constants.Size.Kilobyte:#,#0} KB on average";
        }

        private (long NowMs, double QueueDepth) SampleQueue()
        {
            var nowMs = _clock.ElapsedMilliseconds;
            if (_queueReader == null)
                return (nowMs, _queueDepth.Current);

            var last = _lastSampleMs;
            if (nowMs - last < SampleIntervalMs ||
                Interlocked.CompareExchange(ref _lastSampleMs, nowMs, last) != last)
                return (nowMs, _queueDepth.Current); // not due yet, or another thread samples

            try
            {
                _queueDepth.Update(_queueReader.Read());
            }
            catch
            {
                // a failed read is a lost sample, nothing more
            }

            return (nowMs, _queueDepth.Current);
        }
    }
}
