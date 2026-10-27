using System;
using System.Diagnostics;
using Raven.Server.Storage;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron
{
    public class RavenDB_27822 : NoDisposalNeeded
    {
        public RavenDB_27822(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly CongestionThresholds Thresholds = new(DrainShare: 0.5, JournalWriteLatencyInMs: 50, Sustained: TimeSpan.FromMinutes(10));

        private static DeviceWriteBudget NewDevice() => new(queueReader: null, pathOnDevice: "test",
            syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
            classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);

        [RavenFact(RavenTestCategory.Voron)]
        public void DeviceWriteBudget_reports_drain_switch_and_why_the_class_is_unknown()
        {
            var device = NewDevice();

            Assert.Equal(DeviceWriteBudget.DeviceClass.Unknown, device.Classify(out var reason));
            Assert.Equal(DeviceWriteBudget.ClassReason.NoWrites, reason);
            Assert.Null(device.LastModeSwitch);
            Assert.False(device.IsDraining);

            // tiny journal writes, ~97ms each
            for (var i = 0; i < 16; i++)
                device.RecordJournalWrite(TimeSpan.FromMilliseconds(97).Ticks, 7 * 1024, Stopwatch.GetTimestamp());
            Assert.Equal(DeviceWriteBudget.DeviceClass.Unknown, device.Classify(out reason));
            Assert.Equal(DeviceWriteBudget.ClassReason.WritesTooSmall, reason);

            for (var i = 0; i < 8; i++)
                device.RecordSyncCost(TimeSpan.FromSeconds(1.2).Ticks);
            Assert.True(device.ShouldDrain());

            Assert.True(device.IsDraining);
            Assert.NotNull(device.LastModeSwitch);
            Assert.True(device.TimeSinceLastJournalWrite < TimeSpan.FromMinutes(1));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Device_draining_most_of_the_hour_is_saturated_and_clears_after_an_hour_of_normal()
        {
            var tracker = new DeviceTracker(NewDevice());
            var at = new DateTime(2026, 10, 26, 0, 0, 0, DateTimeKind.Utc);

            // 30 minutes of drain is exactly half of the hour - not more than the 50% threshold yet
            for (var i = 0; i < 180; i++)
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: true, JournalWriteLatencyInMs: 5, QueueDepth: 13), Thresholds);
            Assert.Equal(StorageDeviceStatus.Busy, tracker.GetStatus(Thresholds));

            tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: true, JournalWriteLatencyInMs: 5, QueueDepth: 13), Thresholds);
            Assert.Equal(StorageDeviceStatus.Saturated, tracker.GetStatus(Thresholds));
            // the cause stays while the disk is saturated, so it must not quote a share that goes stale
            for (var i = 0; i < 60; i++)
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: true, JournalWriteLatencyInMs: 5, QueueDepth: 13), Thresholds);
            Assert.Equal("writes were paced more than 50% of an hour because the disk was congested", tracker.SaturationCause);
            var lastBusy = at;

            // normal, but not yet for a full hour
            while (at < lastBusy.AddMinutes(59))
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: 1, QueueDepth: 0.5), Thresholds);
            Assert.Equal(StorageDeviceStatus.Saturated, tracker.GetStatus(Thresholds));

            while (at < lastBusy.AddHours(1))
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: 1, QueueDepth: 0.5), Thresholds);
            Assert.Equal(StorageDeviceStatus.Ok, tracker.GetStatus(Thresholds));
            Assert.Null(tracker.SaturatedSince);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Slow_journal_writes_are_saturated_only_when_sustained_and_idle_breaks_the_streak()
        {
            var tracker = new DeviceTracker(NewDevice());
            var at = new DateTime(2026, 10, 26, 0, 0, 0, DateTimeKind.Utc);

            for (var i = 0; i < 59; i++)
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: 97, QueueDepth: null), Thresholds);
            Assert.Equal(StorageDeviceStatus.Busy, tracker.GetStatus(Thresholds));

            // an idle interval (no journal write) counts as normal - the device latency average is frozen there, not slow
            tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: null, QueueDepth: null), Thresholds);
            Assert.Equal(StorageDeviceStatus.Ok, tracker.GetStatus(Thresholds));

            for (var i = 0; i < 61; i++)
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: 97, QueueDepth: null), Thresholds);
            Assert.Equal(StorageDeviceStatus.Saturated, tracker.GetStatus(Thresholds));
            Assert.Equal("journal writes stayed above 50 ms for 10 minutes", tracker.SaturationCause);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Hint_message_reports_the_last_hour_not_the_latency_of_the_moment()
        {
            var tracker = new DeviceTracker(NewDevice());
            var at = new DateTime(2026, 10, 26, 0, 0, 0, DateTimeKind.Utc);

            // congested most of the hour, journal writes typically 16 ms with one burst
            for (var i = 0; i < 300; i++)
                tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: true, JournalWriteLatencyInMs: i == 150 ? 2000 : 16, QueueDepth: 16), Thresholds);

            // between bursts the average is low again
            tracker.Add(new DeviceTracker.Sample(at = at.AddSeconds(10), Draining: true, JournalWriteLatencyInMs: 3, QueueDepth: 16), Thresholds);

            string message = StorageDeviceMonitor.CreateHintMessage(tracker, slowest: null, tracker.SummarizeLastHour(), Thresholds);

            Assert.StartsWith("Disk test was congested 84% of the last hour (queue depth 16, pacing starts above 5), so the server paced its data file writes. " +
                              "In the last hour journal writes typically took 16 ms (peak 2 s). Writes and indexing are slowed down.", message);

            // calm again, but still saturated until an hour without congestion
            tracker.Add(new DeviceTracker.Sample(at.AddSeconds(10), Draining: false, JournalWriteLatencyInMs: 1, QueueDepth: 0.5), Thresholds);
            message = StorageDeviceMonitor.CreateHintMessage(tracker, slowest: null, tracker.SummarizeLastHour(), Thresholds);
            Assert.Contains("Writes and indexing were slowed down. The disk keeps up again now", message);
        }
    }
}
