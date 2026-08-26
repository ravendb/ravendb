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
                var candidate = new DeviceWriteBudget(reader, pathOnDevice, syncCostThresholdTicks, queueDepthThreshold, classifyAboveLatencyTicks);
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
        // queue depth is kept in per-mille units (1000 x the average in-flight request count),
        // so the fractional aqu-sz signal fits SimpleEwma's longs without losing precision
        private readonly long _enterQueueDepthPerMille;
        private readonly long _exitQueueDepthPerMille;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private long _activeQueueThresholdPerMille; // the enter value while trickling, the exit value while draining
        private Sparrow.Utils.SimpleEwma _syncCostTicksEwma = new(smoothing: 4);
        private Sparrow.Utils.SimpleEwma _queueDepthPerMille = new(smoothing: 4);
        private long _lastSampleMs;
        private long _lastBusyMs;
        private bool _draining;

        internal DeviceWriteBudget(DeviceQueueDepthReader queueReader, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold, long classifyAboveLatencyTicks)
        {
            _queueReader = queueReader;
            _pathOnDevice = pathOnDevice;
            _syncThresholdTicks = syncCostThresholdTicks;
            _enterQueueDepthPerMille = queueDepthThreshold * 1000L;
            _exitQueueDepthPerMille = queueDepthThreshold * 600L; // leave only well below the entry point
            _activeQueueThresholdPerMille = _enterQueueDepthPerMille;
            _classifyAboveLatencyTicks = classifyAboveLatencyTicks;
        }

        public enum DeviceClass
        {
            Unknown, // no evidence yet, go for safe defaults
            Fast,    // example: nvme - very high limits, or we never hit them
            Budgeted // example: gp3 - both bandwidth & IOPS limits that we hit
        }

        // journal write telemetry across EVERY environment on this device
        private Sparrow.Utils.SimpleEwma _journalWriteLatencyTicks = new(smoothing: 8);
        private Sparrow.Utils.SimpleEwma _journalWriteSizeBytes = new(smoothing: 8);
        private long _lastJournalWriteActivityTimestamp;
        private readonly long _classifyAboveLatencyTicks;

        public void RecordJournalWrite(long latencyTicks, long sizeInBytes)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, Stopwatch.GetTimestamp());
            _journalWriteLatencyTicks.Update(latencyTicks);
            _journalWriteSizeBytes.Update(sizeInBytes);
        }

        public void RecordJournalWriteActivity()
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, Stopwatch.GetTimestamp());
        }

        public bool JournalWriteRecentlyActive =>
            Stopwatch.GetTimestamp() - Volatile.Read(ref _lastJournalWriteActivityTimestamp) < RecentWriteActivityWindowTimestampTicks;

        public DeviceClass MeasuredDeviceClass
        {
            get
            {
                // small writes can be fast on a slow device, so we can't estimate from small writes only
                // gp3 writes small batches in 1.3-1.9ms, gp2 in 3-4ms, we need more than that...
                if (_journalWriteSizeBytes.Current < 256 * Voron.Global.Constants.Size.Kilobyte)
                    return DeviceClass.Unknown;

                var ewma = _journalWriteLatencyTicks.Current;
                var threshold = _classifyAboveLatencyTicks;
                if (ewma == 0 || threshold == 0)
                    return DeviceClass.Unknown;

                return ewma < threshold / 2 ? DeviceClass.Fast : DeviceClass.Budgeted;
            }
        }

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
            var (nowMs, queueDepthPerMille) = SampleQueue();
            // either high device queue depth, or high sync cost (too much I/O for the device to keep up)
            if (queueDepthPerMille > _activeQueueThresholdPerMille || _syncCostTicksEwma.Current > _syncThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThresholdPerMille = _exitQueueDepthPerMille;
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The device that holds '{_pathOnDevice}' is congested (queue {queueDepthPerMille / 1000.0:0.0}, " +
                                  $"sync {_syncCostTicksEwma.Current / TimeSpan.TicksPerMillisecond}ms). Every environment on this device " +
                                  "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                                  "in paced blocks before the fdatasync.");
                    }
                }
                return true;
            }
            
            if (_draining is false) 
                return false;

            // we require a quiet period before we exit drain mode, to avoid thrashing back and forth
            if (nowMs - _lastBusyMs < ExitQuietMs)
                return true;

            _draining = false;
            _activeQueueThresholdPerMille = _enterQueueDepthPerMille;
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"The device that holds '{_pathOnDevice}' is quiet again (queue {queueDepthPerMille / 1000.0:0.0}). Every environment " +
                            "on this device returns to trickle mode: flushes start writeback of their dirty ranges, and syncs " +
                            "use a plain fdatasync.");
            }

            return false;
        }

        private (long NowMs, long QueueDepthPerMille) SampleQueue()
        {
            var nowMs = _clock.ElapsedMilliseconds;
            if (_queueReader == null)
                return (nowMs, _queueDepthPerMille.Current);

            var last = _lastSampleMs;
            if (nowMs - last < SampleIntervalMs ||
                Interlocked.CompareExchange(ref _lastSampleMs, nowMs, last) != last)
                return (nowMs, _queueDepthPerMille.Current); // not due yet, or another thread samples

            try
            {
                _queueDepthPerMille.Update((long)(_queueReader.Read() * 1000));
            }
            catch
            {
                // a failed read is a lost sample, nothing more
            }

            return (nowMs, _queueDepthPerMille.Current);
        }
    }
}
