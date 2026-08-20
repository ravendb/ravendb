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
    /// Per-physical-device selector for the data-file writeback mode.
    /// 
    /// * Trickle: each flush starts writeback of its dirty ranges (initiate only) and the sync is a plain fdatasync.
    ///   This is best correct while the device has headroom, since we make maximum utilization of the device's capacity to
    ///   avoid spikes. 
    /// 
    /// * Drain: flushes do not start writeback; the sync pushes the dirty ranges in bounded blocks before the fdatasync().
    ///   Under load, we want to limit how much I/O we generate, and benefit from dead-write merging. 
    ///
    /// 
    /// Using time-weighted device queue depth (the iostat "aqu-sz" number), sampledonce each second through. 
    /// When it exceed queue depth of 5 (default), we assume that we are doing too much I/O and scale back to ensure that 
    /// journal writes (perf critical) are not stalled behind it.
    /// </summary>
    public sealed class WritebackPacingGate
    {
        private static readonly ConcurrentDictionary<ulong, WritebackPacingGate> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<WritebackPacingGate>();

        public static WritebackPacingGate GetForDevice(ulong deviceId, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold)
        {
            return DevicesById.GetOrAdd(deviceId,
                static (id, a) => new WritebackPacingGate(DeviceQueueDepthReader.TryCreate(a.Path, id), a.Path, a.SyncTicks, a.QueueDepth),
                (Path: pathOnDevice, SyncTicks: syncCostThresholdTicks, QueueDepth: queueDepthThreshold));
        }

        private const long SampleIntervalMs = 1000;
        private const long ExitQuietMs = 30_000;

        private readonly DeviceQueueDepthReader _queueReader; // null = no queue signal on this platform
        private readonly string _pathOnDevice;
        private readonly long _syncThresholdTicks;
        private readonly double _enterQueueDepth;
        private readonly double _exitQueueDepth;
        private readonly Stopwatch _clock = Stopwatch.StartNew();

        private double _activeQueueThreshold; // the enter value while trickling, the exit value while draining
        private long _syncCostTicksEwma;
        private double _queueDepthEwma;
        private long _lastSampleMs;
        private long _lastBusyMs;
        private bool _draining;

        internal WritebackPacingGate(DeviceQueueDepthReader queueReader, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold)
        {
            _queueReader = queueReader;
            _pathOnDevice = pathOnDevice;
            _syncThresholdTicks = syncCostThresholdTicks;
            _enterQueueDepth = queueDepthThreshold;
            _exitQueueDepth = queueDepthThreshold * 0.6; // leave only well below the entry point
            _activeQueueThreshold = _enterQueueDepth;
        }

        public long SyncCostTicks => _syncCostTicksEwma;
        public double QueueDepth => _queueDepthEwma;
        public bool HasQueueSignal => _queueReader != null;

        public void RecordSyncCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = _syncCostTicksEwma;
            _syncCostTicksEwma = current == 0 ? ticks : current + (ticks - current) / 4;
        }

        public bool ShouldDrain()
        {
            var (nowMs, queueDepth) = SampleQueue();

            if (queueDepth > _activeQueueThreshold || _syncCostTicksEwma > _syncThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThreshold = _exitQueueDepth;
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The device that holds '{_pathOnDevice}' is congested (queue {queueDepth:0.0}, " +
                                  $"sync {_syncCostTicksEwma / TimeSpan.TicksPerMillisecond}ms). Every environment on this device " +
                                  "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                                  "in paced blocks before the fdatasync.");
                    }
                }
                return true;
            }
            
            if (_draining is false) 
                return false;

            if (nowMs - _lastBusyMs < ExitQuietMs)
                return true;

            _draining = false;
            _activeQueueThreshold = _enterQueueDepth;
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"The device that holds '{_pathOnDevice}' is quiet again (queue {queueDepth:0.0}). Every environment " +
                            "on this device returns to trickle mode: flushes start writeback of their dirty ranges, and syncs " +
                            "use a plain fdatasync.");
            }

            return false;
        }

        private (long NowMs, double QueueDepth) SampleQueue()
        {
            var nowMs = _clock.ElapsedMilliseconds;
            var queueDepth = _queueDepthEwma;
            if (_queueReader == null)
                return (nowMs, queueDepth);

            var last = _lastSampleMs;
            if (nowMs - last < SampleIntervalMs ||
                Interlocked.CompareExchange(ref _lastSampleMs, nowMs, last) != last)
                return (nowMs, queueDepth); // not due yet, or another thread samples

            try
            {
                var value = _queueReader.Read();
                queueDepth = queueDepth == 0 ? value : queueDepth + (value - queueDepth) / 4;
                _queueDepthEwma = queueDepth;
            }
            catch
            {
                // a torn read is a lost sample, nothing more
            }

            return (nowMs, queueDepth);
        }
    }
}
