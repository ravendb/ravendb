using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Storage;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27826 : RavenTestBase
{
    public RavenDB_27826(ITestOutputHelper output) : base(output)
    {
    }

    private static readonly DeviceTracker.Sample Congested = new(default, Draining: true, JournalWriteLatencyInMs: 97, QueueDepth: 13);
    private static readonly DeviceTracker.Sample Healthy = new(default, Draining: false, JournalWriteLatencyInMs: 1, QueueDepth: 0.5);

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Saturated_disk_raises_one_server_hint_with_evidence_instead_of_per_database_slow_io_and_clears_after_an_hour()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        var database = await GetDatabase(server, store.Database);
        var monitor = server.ServerStore.StorageDeviceMonitor;

        DeviceWriteBudget device = database.DocumentsStorage.Environment.Options.DeviceWriteBudget;
        Assert.NotNull(device);

        var now = DateTime.UtcNow;
        var sample = Congested;
        monitor.ForTestingPurposesOnly().UtcNow = () => now;
        monitor.ForTestingPurposesOnly().SampleOverride = d => ReferenceEquals(d, device) ? sample : Healthy;

        // 10 minutes of slow journal writes (PerformanceHints.Storage.CongestionSustainedInMin)
        for (var i = 0; i <= 61; i++)
        {
            now = now.AddSeconds(10);
            monitor.Sample();
        }

        var tracker = monitor.GetTracker(device);
        Assert.Equal(StorageDeviceStatus.Saturated, tracker.GetStatus(monitor.Thresholds));

        var hintId = PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, tracker.MountPoint);
        Assert.True(server.ServerStore.NotificationCenter.Exists(hintId));
        Assert.Null(server.ServerStore.NotificationCenter.GetDatabaseFor(hintId)); // server level: Operators only

        // a slow journal write on that disk is evidence on the hint, not a per-database Slow IO hint
        database.CheckWriteRateAndNotifyIfNecessary(SlowJournalWrite(database));
        database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);
        Assert.False(database.NotificationCenter.Exists(PerformanceHint.GetKey(PerformanceHintReason.SlowIO, "slow-writes")));
        Assert.Single(tracker.GetSlowWrites(now));

        // an hour without a busy sample clears it
        sample = Healthy;
        for (var i = 0; i < 360; i++)
        {
            now = now.AddSeconds(10);
            monitor.Sample();
        }

        Assert.Equal(StorageDeviceStatus.Ok, tracker.GetStatus(monitor.Thresholds));
        Assert.False(server.ServerStore.NotificationCenter.Exists(hintId));
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task When_the_hint_is_filtered_out_slow_writes_still_raise_the_per_database_slow_io()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> { ["Notifications.FilterOut"] = nameof(PerformanceHintReason.StorageCongestion) }
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        var database = await GetDatabase(server, store.Database);
        var monitor = server.ServerStore.StorageDeviceMonitor;

        DeviceWriteBudget device = database.DocumentsStorage.Environment.Options.DeviceWriteBudget;
        Assert.NotNull(device);

        var now = DateTime.UtcNow;
        monitor.ForTestingPurposesOnly().UtcNow = () => now;
        monitor.ForTestingPurposesOnly().SampleOverride = d => ReferenceEquals(d, device) ? Congested : Healthy;

        for (var i = 0; i <= 61; i++)
        {
            now = now.AddSeconds(10);
            monitor.Sample();
        }

        var tracker = monitor.GetTracker(device);
        Assert.Equal(StorageDeviceStatus.Saturated, tracker.GetStatus(monitor.Thresholds));
        Assert.False(server.ServerStore.NotificationCenter.Exists(PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, tracker.MountPoint)));

        database.CheckWriteRateAndNotifyIfNecessary(SlowJournalWrite(database));
        database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);
        Assert.True(database.NotificationCenter.Exists(PerformanceHint.GetKey(PerformanceHintReason.SlowIO, "slow-writes")));
    }

    private static IoChange SlowJournalWrite(DocumentDatabase database)
    {
        var start = DateTime.UtcNow;
        return new IoChange
        {
            FileName = database.DocumentsStorage.Environment.Options.JournalPath.Combine("0000000000000000001.journal").FullPath,
            MeterItem = new IoMeterBuffer.MeterItem
            {
                Type = IoMetrics.MeterType.JournalWrite,
                Size = 4096,
                Start = start,
                End = start + TimeSpan.FromSeconds(2)
            }
        };
    }
}
