using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Sparrow.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;
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
                DateTime now = ForTestingPurposes?.UtcNow?.Invoke() ?? DateTime.UtcNow;
                CongestionThresholds thresholds = Thresholds;

                foreach (DeviceWriteBudget device in GetDevices())
                {
                    DeviceTracker tracker = _trackers.GetOrAdd(device, d => new DeviceTracker(d));
                    DeviceTracker.Sample sample = ForTestingPurposes?.SampleOverride?.Invoke(device) ?? DeviceTracker.Sample.From(device, now, SampleInterval);
                    tracker.Add(sample with { At = now }, thresholds);
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

        public DeviceTracker GetTracker(DeviceWriteBudget device) => device != null && _trackers.TryGetValue(device, out DeviceTracker tracker) ? tracker : null;

        public HashSet<DeviceWriteBudget> GetDevices()
        {
            var devices = new HashSet<DeviceWriteBudget>(ReferenceEqualityComparer.Instance);
            foreach (EnvironmentOfDatabase env in GetEnvironments())
            {
                DeviceWriteBudget device = env.Environment.Environment.Options.DeviceWriteBudget;
                if (device != null) // the device could not be identified, the environment uses its own budget - nothing to track per device
                    devices.Add(device);
            }

            return devices;
        }

        public readonly record struct EnvironmentOfDatabase(string Database, StorageEnvironmentWithType Environment);

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

                    yield return new EnvironmentOfDatabase(database.Name, env);
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

        public DeviceTracker(DeviceWriteBudget device)
        {
            Device = device;
        }

        public DeviceWriteBudget Device { get; }

        public DateTime? SaturatedSince { get; private set; }

        public string SaturationCause { get; private set; }

        public string MountPoint => _mountPoint ??= DiskUtils.GetDiskSpaceInfoForFile(Device.PathOnDevice)?.DriveName ?? Device.PathOnDevice;

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

        // used after a restart: a saturation reported before it stays until the device proves an hour of normal
        public void MarkSaturated(DateTime since, string cause)
        {
            lock (_lock)
            {
                SaturatedSince ??= since;
                SaturationCause ??= cause;
                _lastBusyAt ??= since;
            }
        }

        public StorageDeviceStatus GetStatus(CongestionThresholds thresholds)
        {
            lock (_lock)
            {
                if (SaturatedSince != null)
                    return StorageDeviceStatus.Saturated;

                return _samples.Count > 0 && LastSample.IsBusy(thresholds.JournalWriteLatencyInMs) ? StorageDeviceStatus.Busy : StorageDeviceStatus.Ok;
            }
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

            double drainShare = draining / (double)Capacity;
            if (drainShare > thresholds.DrainShare)
                return $"writes were paced because the disk was congested {drainShare:P0} of the last hour";

            // every sample of the sustained period (and it must be fully covered) is slower than the threshold
            DateTime from = now - thresholds.Sustained;
            bool covered = false;
            double sum = 0;
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

                sum += latency;
                count++;
            }

            if (covered == false || count == 0)
                return null;

            return $"journal writes averaged {sum / count:#,#0} ms for the last {thresholds.Sustained.TotalMinutes:#,#0} minutes " +
                   $"(threshold {thresholds.JournalWriteLatencyInMs:#,#0} ms)";
        }
    }
}
