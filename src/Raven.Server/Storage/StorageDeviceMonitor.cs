using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Server.Meters;
using Voron;
using Voron.Impl.Journal;

namespace Raven.Server.Storage
{
    public enum StorageDeviceStatus
    {
        Ok,
        Busy,
        Saturated
    }

    /// <summary>
    /// Samples the per-device write state (<see cref="DeviceWriteBudget"/>) of every disk this server writes to, keeps the last hour,
    /// and decides whether a disk is OK, busy right now, or saturated (busy for long enough to matter).
    /// Sampling is done here, off the write path, so Voron pays nothing for this visibility.
    /// </summary>
    public sealed class StorageDeviceMonitor : IDisposable
    {
        public static readonly TimeSpan SampleInterval = TimeSpan.FromSeconds(10);

        private readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<StorageDeviceMonitor>();
        private readonly ServerStore _serverStore;
        private readonly ConcurrentDictionary<DeviceWriteBudget, DeviceTracker> _trackers = new(ReferenceEqualityComparer.Instance);
        private readonly object _sampleLock = new();
        private Timer _timer;

        public StorageDeviceMonitor(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _timer = new Timer(_ => Sample(), null, SampleInterval, SampleInterval);
        }

        public CongestionThresholds Thresholds => CongestionThresholds.From(_serverStore.Configuration.PerformanceHints);

        internal void Sample()
        {
            if (Monitor.TryEnter(_sampleLock) == false)
                return;

            try
            {
                DateTime now = UtcNow;
                CongestionThresholds thresholds = Thresholds;
                _startedAt ??= now;
                RestorePersistedHintsOnce();

                foreach (var (device, mountPoint) in GetDevices())
                {
                    DeviceTracker tracker = _trackers.GetOrAdd(device, d => new DeviceTracker(d, mountPoint));
                    if (tracker.HintRaised == false && _persistedHints.Remove(tracker.MountPoint))
                        tracker.RestorePersistedHint(now);

                    DeviceTracker.Sample sample = ForTestingPurposes?.SampleOverride?.Invoke(device) ?? DeviceTracker.Sample.From(device, now, SampleInterval);
                    tracker.Add(sample with { At = now }, thresholds);

                    LogClassChange(tracker);
                    UpdateHint(tracker, now);
                }

                // a hint from before the restart for a disk no database writes to anymore: give it the same hour, then let it go
                if (now - _startedAt.Value >= CongestionThresholds.Window)
                {
                    foreach (string mountPoint in _persistedHints)
                        _serverStore.NotificationCenter.Dismiss(PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, mountPoint), sendNotificationEvenIfDoesntExist: false);
                    _persistedHints.Clear();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn("Failed to sample the storage devices state", e);
            }
            finally
            {
                Monitor.Exit(_sampleLock);
            }
        }

        private DateTime UtcNow => ForTestingPurposes?.UtcNow?.Invoke() ?? DateTime.UtcNow;

        private static readonly TimeSpan HintRefreshInterval = TimeSpan.FromMinutes(10);
        private static readonly TimeSpan HintEvidenceRefreshInterval = TimeSpan.FromMinutes(1);

        private void UpdateHint(DeviceTracker tracker, DateTime now)
        {
            string id = PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, tracker.MountPoint);

            if (tracker.SaturatedSince == null)
            {
                if (tracker.HintRaised == false)
                    return;

                // a hint from before the restart is kept as it was until the disk shows an hour without congestion
                if (tracker.HintRestored && tracker.IsQuietForAnHour(now) == false)
                    return;

                tracker.HintRaised = false;
                tracker.HintStored = false;
                tracker.HintRestored = false;
                _serverStore.NotificationCenter.Dismiss(id, sendNotificationEvenIfDoesntExist: false);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"The disk {tracker.MountPoint} is back to normal for an hour, the storage congestion hint was cleared.");
                return;
            }

            // a live saturation replaces a hint from before the restart right away
            bool first = tracker.HintRaised == false || tracker.HintRestored;
            bool due = first ||
                       now - tracker.LastHintUpdate >= HintRefreshInterval ||
                       (tracker.HasNewEvidence && now - tracker.LastHintUpdate >= HintEvidenceRefreshInterval);
            if (due == false || _serverStore.NotificationCenter.IsInitialized == false)
                return;

            PerformanceHint hint = CreateHint(tracker);
            tracker.HintStored = _serverStore.Configuration.Notifications.ShouldFilterOut(hint) == false;
            _serverStore.NotificationCenter.Add(hint);
            tracker.HintRaised = true;
            tracker.HintRestored = false;
            tracker.LastHintUpdate = now;

            if (first && _logger.IsInfoEnabled)
                _logger.Info($"{hint.Title}: {hint.Message}");
        }

        private PerformanceHint CreateHint(DeviceTracker tracker)
        {
            DeviceWriteBudget device = tracker.Device;
            DeviceTracker.LastHour lastHour = tracker.SummarizeLastHour();

            StorageCongestionDetails details = tracker.GetEvidence();
            details.NodeTag = _serverStore.NodeTag;
            details.MountPoint = tracker.MountPoint;
            details.PathOnDevice = device.PathOnDevice;
            details.Cause = tracker.SaturationCause;
            details.SaturatedSince = tracker.SaturatedSince ?? UtcNow;
            details.DrainShareOfLastHour = tracker.DrainShareOfLastHour;
            details.JournalWriteLatencyInMs = device.JournalWriteLatency.TotalMilliseconds;
            details.TypicalJournalWriteLatencyInMs = lastHour.TypicalJournalWriteLatencyInMs;
            details.PeakJournalWriteLatencyInMs = lastHour.PeakJournalWriteLatencyInMs;
            details.SyncCostInMs = device.SyncCost.TotalMilliseconds;
            details.SlowWritesMeasured = _serverStore.Configuration.Storage.EnableIoMetrics;

            return PerformanceHint.Create(null, "Storage is saturated", CreateHintMessage(tracker, details, lastHour, Thresholds), PerformanceHintReason.StorageCongestion,
                NotificationSeverity.Info, tracker.MountPoint, details);
        }

        // the evidence of the last hour, not the value of the moment: the latency average swings between bursts
        internal static string CreateHintMessage(DeviceTracker tracker, StorageCongestionDetails details, DeviceTracker.LastHour lastHour, CongestionThresholds thresholds)
        {
            var sentences = new List<string>();

            double drainShare = tracker.DrainShareOfLastHour;
            double enterQueueDepth = tracker.Device.EnterDrainQueueDepth;
            if (drainShare > thresholds.DrainShare)
            {
                string queue = lastHour.TypicalQueueDepth is { } queueDepth && queueDepth > enterQueueDepth
                    ? $" (queue depth {queueDepth:#,#0.#}, pacing starts above {enterQueueDepth:#,#0.#})"
                    : string.Empty;
                sentences.Add($"Disk {tracker.MountPoint} was congested {drainShare * 100:0}% of the last hour{queue}, so the server paced its data file writes.");
            }
            else
            {
                sentences.Add($"On disk {tracker.MountPoint}, {tracker.SaturationCause}.");
            }

            if (lastHour.TypicalJournalWriteLatencyInMs is { } typical && lastHour.PeakJournalWriteLatencyInMs is { } peak)
                sentences.Add(DescribeLastHour(typical, peak, details.Writes.Values.MaxBy(x => x.DurationInSec)));

            // a saturated disk stays saturated for an hour after it calms down
            sentences.Add(tracker.IsBusyNow(thresholds)
                ? "Writes and indexing are slowed down."
                : "Writes and indexing were slowed down. The disk keeps up again now, this clears after an hour without congestion.");
            sentences.Add("Consider a volume with more IOPS / throughput, an instance with higher storage bandwidth, or more RAM to reduce disk reads.");

            return string.Join(" ", sentences);
        }

        // shared by the hint and the debug package analyzer, so both tell the same story
        internal static string DescribeLastHour(double typicalLatencyInMs, double peakLatencyInMs, SlowIoDetails.SlowWriteInfo slowest)
        {
            string slowestWrite = slowest == null
                ? string.Empty
                : $", and the slowest {Describe(slowest.Type)} took {FormatMs(slowest.DurationInSec * 1000)} for {FormatSize(slowest.DataWrittenInMb)}";
            return $"In the last hour journal writes typically took {FormatMs(typicalLatencyInMs)} (peak {FormatMs(peakLatencyInMs)}){slowestWrite}.";

            static string FormatSize(double mb) => mb >= 1 ? $"{mb:#,#0.#} MB" : $"{mb * 1024:#,#0} KB";

            static string Describe(IoMetrics.MeterType type) => type switch
            {
                IoMetrics.MeterType.JournalWrite => "journal write",
                IoMetrics.MeterType.DataFlush => "data flush",
                IoMetrics.MeterType.DataSync => "data sync",
                _ => "write"
            };
        }

        internal static string FormatMs(double ms) => ms >= 1000 ? $"{ms / 1000:#,#0.#} s" : $"{ms:#,#0.#} ms";

        /// <summary>
        /// Called for every slow write a database detects. The write always becomes evidence for its disk; returns true when the
        /// congestion hint of that disk is shown, so the database does not raise its own Slow IO hint for the same problem.
        /// </summary>
        public bool RecordSlowWrite(DeviceWriteBudget device, IoChange ioChange)
        {
            DeviceTracker tracker = GetTracker(device);
            if (tracker == null)
                return false;

            tracker.RecordSlowWrite(ioChange, UtcNow);
            return tracker.HintStored;
        }

        // touched by the sampler only, under _sampleLock
        private readonly HashSet<string> _persistedHints = new();
        private bool _persistedHintsRestored;
        private DateTime? _startedAt;

        // a hint stored before a restart is history: it stays in the notification center until the disk proves an hour of normal,
        // but the state of the disk (dashboard, Storage I/O, endpoint) only comes from the samples taken since the restart
        private void RestorePersistedHintsOnce()
        {
            if (_persistedHintsRestored || _serverStore.NotificationCenter.IsInitialized == false)
                return;

            _persistedHintsRestored = true;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (NotificationTableValue hint in _serverStore.NotificationCenter.Storage.ReadActionsOfType(context, NotificationType.PerformanceHint))
                {
                    using (hint)
                    {
                        if ((PerformanceHintReason)Bits.SwapBytes(hint.Reason) != PerformanceHintReason.StorageCongestion)
                            continue;

                        if (hint.Json.TryGet(nameof(PerformanceHint.Source), out string mountPoint) && mountPoint != null)
                            _persistedHints.Add(mountPoint);
                    }
                }
            }
        }

        // the class is a getter evaluated per transaction, watching it here keeps the change detection off the write path
        private const int StableClassSamples = 3;

        private void LogClassChange(DeviceTracker tracker)
        {
            DeviceWriteBudget device = tracker.Device;
            DeviceWriteBudget.DeviceClass current = device.Classify(out DeviceWriteBudget.ClassReason reason);

            // a latency sitting right at a class boundary flips the class back and forth; report it once it held for a few samples
            if (current != tracker.CandidateClass)
            {
                tracker.CandidateClass = current;
                tracker.CandidateSamples = 0;
            }

            if (++tracker.CandidateSamples < StableClassSamples)
                return;

            DeviceWriteBudget.DeviceClass? previous = tracker.LastClass;
            tracker.LastClass = current;

            if (previous == current || (previous == null && current == DeviceWriteBudget.DeviceClass.Unknown) || _logger.IsInfoEnabled == false)
                return;

            JournalCompressionAlgorithm auto = current == DeviceWriteBudget.DeviceClass.Budgeted ? JournalCompressionAlgorithm.Zstd : JournalCompressionAlgorithm.Lz4;
            _logger.Info($"The device that holds '{device.PathOnDevice}' is now classified as {current} (was {previous?.ToString() ?? "not classified"}): {Describe(reason, device)}. " +
                         $"Environments on this device with Auto journal compression use {auto}.");
        }

        public static string Describe(DeviceWriteBudget.ClassReason reason, DeviceWriteBudget device)
        {
            string latency = $"{device.JournalWriteLatency.TotalMilliseconds:#,#0.##} ms";
            string size = $"{device.JournalWriteSizeInBytes / 1024:#,#0} KB";
            string threshold = $"{device.ClassifyAboveLatency.TotalMilliseconds:#,#0.##} ms";
            return reason switch
            {
                DeviceWriteBudget.ClassReason.NoWrites => "no journal writes measured yet",
                DeviceWriteBudget.ClassReason.VeryLowLatency => $"journal writes take {latency}, below 1/8 of {threshold}",
                DeviceWriteBudget.ClassReason.WritesTooSmall => $"journal writes average {size}, below {DeviceWriteBudget.SmallWriteBytes / 1024:#,#0} KB, too small to tell a slow disk from a fast one ({latency})",
                DeviceWriteBudget.ClassReason.LowLatency => $"journal writes of {size} take {latency}, below half of {threshold}",
                DeviceWriteBudget.ClassReason.HighLatency => $"journal writes of {size} take {latency}, at or above half of {threshold}",
                _ => reason.ToString()
            };
        }

        public ICollection<DeviceTracker> Trackers => _trackers.Values;

        public DeviceTracker GetTracker(DeviceWriteBudget device) => device != null && _trackers.TryGetValue(device, out DeviceTracker tracker) ? tracker : null;

        // device -> mount point (drive on Windows) of the first environment found on it
        public Dictionary<DeviceWriteBudget, string> GetDevices()
        {
            var devices = new Dictionary<DeviceWriteBudget, string>(ReferenceEqualityComparer.Instance);
            foreach (EnvironmentOfDatabase env in GetEnvironments())
            {
                StorageEnvironmentOptions options = env.Environment.Environment.Options;
                DeviceWriteBudget device = options.DeviceWriteBudget;
                if (device == null || devices.ContainsKey(device)) // null: the device could not be identified, the environment uses its own budget
                    continue;

                string mountPoint = null;
                try
                {
                    mountPoint = options.DriveInfoByPath?.Value?.BasePath?.DriveName;
                }
                catch
                {
                    // best effort, falls back to the path root
                }

                devices[device] = mountPoint;
            }

            return devices;
        }

        public readonly record struct EnvironmentOfDatabase(string Database, StorageEnvironmentWithType Environment, DocumentDatabase Instance = null);

        // loaded databases only, never wakes up an idle database
        public IEnumerable<EnvironmentOfDatabase> GetEnvironments()
        {
            StorageEnvironment systemEnv = _serverStore._env;
            if (systemEnv != null)
                yield return new EnvironmentOfDatabase(null, new StorageEnvironmentWithType("<System>", StorageEnvironmentWithType.StorageEnvironmentType.System, systemEnv));

            if (_serverStore.DatabasesLandlord?.DatabasesCache is not { } databases)
                yield break;

            foreach (var kvp in databases)
            {
                var databaseTask = kvp.Value;
                if (databaseTask == null || databaseTask.IsCompletedSuccessfully == false)
                    continue;

                var database = databaseTask.Result;
                foreach (StorageEnvironmentWithType env in database.GetAllStoragesEnvironment())
                {
                    if (env.Environment == null || env.Environment.Disposed)
                        continue;

                    yield return new EnvironmentOfDatabase(database.Name, env, database);
                }
            }
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly() => ForTestingPurposes ??= new TestingStuff();

        internal sealed class TestingStuff
        {
            internal Func<DateTime> UtcNow;

            internal Func<DeviceWriteBudget, DeviceTracker.Sample?> SampleOverride;
        }
    }

    public readonly record struct CongestionThresholds(double DrainShare, double JournalWriteLatencyInMs, TimeSpan Sustained)
    {
        public static readonly TimeSpan Window = TimeSpan.FromHours(1);

        public static CongestionThresholds From(PerformanceHintsConfiguration configuration) => new(
            configuration.StorageCongestionDrainSharePercent / 100.0,
            configuration.StorageCongestionJournalWriteLatency.AsTimeSpan.TotalMilliseconds,
            configuration.StorageCongestionSustained.AsTimeSpan);
    }

    /// <summary>
    /// The last hour of samples of one device and its congestion state. Raise: paced (drain) more than the configured share of the hour,
    /// or journal writes slower than the threshold for the sustained period. Clear: an hour without a single busy sample.
    /// </summary>
    public sealed class DeviceTracker
    {
        public readonly record struct Sample(DateTime At, bool Draining, double? JournalWriteLatencyInMs, double? QueueDepth)
        {
            public static Sample From(DeviceWriteBudget device, DateTime now, TimeSpan interval) => new(
                now,
                device.IsDraining,
                // the device latency average never expires, on an idle device it freezes at the last value, so it only counts with recent writes
                device.TimeSinceLastJournalWrite <= interval ? device.JournalWriteLatency.TotalMilliseconds : null,
                device.HasQueueDepthSignal ? device.QueueDepth : null);

            public bool IsBusy(double latencyThresholdInMs) => Draining || JournalWriteLatencyInMs > latencyThresholdInMs;
        }

        private static readonly int Capacity = (int)(CongestionThresholds.Window.Ticks / StorageDeviceMonitor.SampleInterval.Ticks);

        private readonly object _lock = new();
        private readonly Queue<Sample> _samples = new(Capacity + 1);
        private DateTime? _lastBusyAt;
        private string _mountPoint;

        public DeviceTracker(DeviceWriteBudget device, string mountPoint = null)
        {
            Device = device;
            _mountPoint = string.IsNullOrEmpty(mountPoint) ? null : mountPoint;
        }

        public DeviceWriteBudget Device { get; }

        internal DeviceWriteBudget.DeviceClass? LastClass { get; set; }

        internal DeviceWriteBudget.DeviceClass? CandidateClass { get; set; }

        internal int CandidateSamples { get; set; }

        public DateTime? SaturatedSince { get; private set; }

        public string SaturationCause { get; private set; }

        public string MountPoint => _mountPoint ??= Path.GetPathRoot(Device.PathOnDevice) is { Length: > 0 } root ? root : Device.PathOnDevice;

        public void Add(Sample sample, CongestionThresholds thresholds)
        {
            lock (_lock)
            {
                _samples.Enqueue(sample);
                while (_samples.Count > Capacity || _samples.Peek().At <= sample.At - CongestionThresholds.Window)
                    _samples.Dequeue();

                if (sample.IsBusy(thresholds.JournalWriteLatencyInMs))
                    _lastBusyAt = sample.At;

                if (SaturatedSince == null)
                {
                    SaturationCause = GetSaturationCause(sample.At, thresholds);
                    if (SaturationCause != null)
                        SaturatedSince = sample.At;
                }
                else if (_lastBusyAt == null || sample.At - _lastBusyAt.Value >= CongestionThresholds.Window)
                {
                    SaturatedSince = null;
                    SaturationCause = null;
                }
            }
        }

        // used after a restart: the hint stored before it is kept until an hour without congestion since the restart
        public void RestorePersistedHint(DateTime restartedAt)
        {
            lock (_lock)
            {
                _lastBusyAt ??= restartedAt;
                HintRaised = true;
                HintStored = true;
                HintRestored = true;
            }
        }

        public bool IsQuietForAnHour(DateTime now)
        {
            lock (_lock)
                return _lastBusyAt == null || now - _lastBusyAt.Value >= CongestionThresholds.Window;
        }

        // state of the congestion hint of this disk, owned by the sampler
        internal bool HintRaised { get; set; }

        // the hint shown is the one stored before the restart, not one raised from the samples since
        internal bool HintRestored { get; set; }

        internal bool HintStored { get; set; }

        internal DateTime LastHintUpdate { get; set; }

        private const int MaxSlowWrites = 50;
        private readonly Dictionary<string, SlowIoDetails.SlowWriteInfo> _slowWrites = new();
        private bool _newEvidence;

        internal bool HasNewEvidence
        {
            get
            {
                lock (_lock)
                    return _newEvidence;
            }
        }

        // the worst slow write per file within the last hour
        public void RecordSlowWrite(IoChange ioChange, DateTime now)
        {
            lock (_lock)
            {
                PruneSlowWrites(now);

                var info = new SlowIoDetails.SlowWriteInfo(ioChange, ioChange.MeterItem.End);
                if (_slowWrites.TryGetValue(info.Key, out SlowIoDetails.SlowWriteInfo existing) && existing.DurationInSec >= info.DurationInSec)
                    return;

                _slowWrites[info.Key] = info;
                _newEvidence = true;

                if (_slowWrites.Count > MaxSlowWrites)
                    _slowWrites.Remove(_slowWrites.MinBy(x => x.Value.DurationInSec).Key);
            }
        }

        public StorageCongestionDetails GetEvidence()
        {
            lock (_lock)
            {
                _newEvidence = false;
                return new StorageCongestionDetails { Writes = new Dictionary<string, SlowIoDetails.SlowWriteInfo>(_slowWrites) };
            }
        }

        public List<SlowIoDetails.SlowWriteInfo> GetSlowWrites(DateTime now)
        {
            lock (_lock)
            {
                PruneSlowWrites(now);
                return _slowWrites.Values.OrderByDescending(x => x.DurationInSec).ToList();
            }
        }

        private void PruneSlowWrites(DateTime now)
        {
            foreach (var kvp in _slowWrites)
            {
                if (now - kvp.Value.Date > CongestionThresholds.Window)
                    _slowWrites.Remove(kvp.Key);
            }
        }

        public StorageDeviceStatus GetStatus(CongestionThresholds thresholds)
        {
            lock (_lock)
            {
                if (SaturatedSince != null)
                    return StorageDeviceStatus.Saturated;

                return IsBusyNow(thresholds) ? StorageDeviceStatus.Busy : StorageDeviceStatus.Ok;
            }
        }

        public bool IsBusyNow(CongestionThresholds thresholds)
        {
            lock (_lock)
                return _samples.Count > 0 && LastSample.IsBusy(thresholds.JournalWriteLatencyInMs);
        }

        private Sample LastSample
        {
            get
            {
                Sample last = default;
                foreach (Sample s in _samples)
                    last = s;
                return last;
            }
        }

        // share of the full hour (not of the samples we have), so a fresh start needs the same absolute time in drain
        public double DrainShareOfLastHour
        {
            get
            {
                lock (_lock)
                {
                    int draining = 0;
                    foreach (Sample s in _samples)
                    {
                        if (s.Draining)
                            draining++;
                    }

                    return Math.Min(1.0, draining / (double)Capacity);
                }
            }
        }

        public readonly record struct LastHour(double? TypicalJournalWriteLatencyInMs, double? PeakJournalWriteLatencyInMs, double? TypicalQueueDepth);

        // typical = median of the 10 s samples; intervals without a journal write have no latency and are skipped
        public LastHour SummarizeLastHour()
        {
            var latencies = new List<double>();
            var queueDepths = new List<double>();
            lock (_lock)
            {
                foreach (Sample s in _samples)
                {
                    if (s.JournalWriteLatencyInMs is { } latency)
                        latencies.Add(latency);
                    if (s.QueueDepth is { } queueDepth)
                        queueDepths.Add(queueDepth);
                }
            }

            return new LastHour(Median(latencies), latencies.Count > 0 ? latencies.Max() : null, Median(queueDepths));
        }

        private static double? Median(List<double> values)
        {
            if (values.Count == 0)
                return null;

            values.Sort();
            return values[values.Count / 2];
        }

        public List<Sample> GetHistory()
        {
            lock (_lock)
                return new List<Sample>(_samples);
        }

        private string GetSaturationCause(DateTime now, CongestionThresholds thresholds)
        {
            int draining = 0;
            foreach (Sample s in _samples)
            {
                if (s.Draining)
                    draining++;
            }

            // the cause is kept for as long as the disk stays saturated, so it names the threshold that was crossed, not a measurement
            // that goes stale; the current numbers are reported next to it
            double drainShare = draining / (double)Capacity;
            if (drainShare > thresholds.DrainShare)
                return $"writes were paced more than {thresholds.DrainShare * 100:0}% of an hour because the disk was congested";

            // every sample of the sustained period (and it must be fully covered) is slower than the threshold
            DateTime from = now - thresholds.Sustained;
            bool covered = false;
            int count = 0;
            foreach (Sample s in _samples)
            {
                if (s.At <= from)
                {
                    covered = true;
                    continue;
                }

                if (s.JournalWriteLatencyInMs is not { } latency || latency <= thresholds.JournalWriteLatencyInMs)
                    return null;

                count++;
            }

            if (covered == false || count == 0)
                return null;

            return $"journal writes stayed above {thresholds.JournalWriteLatencyInMs:#,#0} ms for {thresholds.Sustained.TotalMinutes:#,#0} minutes";
        }
    }
}
