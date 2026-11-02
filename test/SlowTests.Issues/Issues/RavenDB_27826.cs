using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Storage;
using Sparrow.Platform;
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
    public async Task Saturated_disk_raises_one_server_hint_with_evidence_and_clears_after_an_hour()
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

        // a slow journal write on that disk is evidence on the hint, and the database keeps its own Slow IO hint for its users
        database.CheckWriteRateAndNotifyIfNecessary(SlowJournalWrite(database));
        database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);
        Assert.True(database.NotificationCenter.Exists(PerformanceHint.GetKey(PerformanceHintReason.SlowIO, "slow-writes")));
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
    public async Task Hint_of_a_disk_no_database_writes_to_anymore_clears_after_an_hour()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, DeleteDatabaseOnDispose = false });
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
        var hintId = PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, tracker.MountPoint);
        Assert.True(server.ServerStore.NotificationCenter.Exists(hintId));

        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));
        Assert.True(await WaitForValueAsync(() => monitor.GetDevices().ContainsKey(device) == false, true));

        for (var i = 0; i < 360; i++)
        {
            now = now.AddSeconds(10);
            monitor.Sample();
        }

        Assert.False(server.ServerStore.NotificationCenter.Exists(hintId));
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Hint_from_before_a_restart_stays_as_history_while_the_disk_status_is_live()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        var database = await GetDatabase(server, store.Database);

        DeviceWriteBudget device = database.DocumentsStorage.Environment.Options.DeviceWriteBudget;
        Assert.NotNull(device);

        // the hint stored before the restart
        server.ServerStore.StorageDeviceMonitor.Sample();
        string mountPoint = server.ServerStore.StorageDeviceMonitor.GetTracker(device).MountPoint;
        string hintId = PerformanceHint.GetKey(PerformanceHintReason.StorageCongestion, mountPoint);
        server.ServerStore.NotificationCenter.Add(PerformanceHint.Create(null, "Storage is saturated", "stored before the restart",
            PerformanceHintReason.StorageCongestion, NotificationSeverity.Info, mountPoint, new StorageCongestionDetails()));

        // a monitor starting now, as after a restart
        using var monitor = new StorageDeviceMonitor(server.ServerStore);
        var now = DateTime.UtcNow;
        monitor.ForTestingPurposesOnly().UtcNow = () => now;
        monitor.ForTestingPurposesOnly().SampleOverride = _ => Healthy;
        monitor.Sample();

        Assert.Equal(StorageDeviceStatus.Ok, monitor.GetTracker(device).GetStatus(monitor.Thresholds));
        Assert.True(server.ServerStore.NotificationCenter.Exists(hintId));

        for (var i = 0; i < 359; i++)
        {
            now = now.AddSeconds(10);
            monitor.Sample();
        }
        Assert.True(server.ServerStore.NotificationCenter.Exists(hintId));

        // an hour without congestion since the restart clears it
        now = now.AddSeconds(10);
        monitor.Sample();
        Assert.False(server.ServerStore.NotificationCenter.Exists(hintId));
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Journals_linked_to_another_disk_are_named_and_not_evidence_for_the_data_disk()
    {
        string dataPath = NewDataPath(forceCreateDir: true);
        string journalsTarget = Path.Combine(Path.GetTempPath(), $"{nameof(RavenDB_27826)}-{Guid.NewGuid():N}");
        if (PlatformDetails.RunningOnPosix || string.Equals(Path.GetPathRoot(dataPath), Path.GetPathRoot(journalsTarget), StringComparison.OrdinalIgnoreCase))
            Assert.Skip("Needs the temp directory on another drive than the test data, to link the Journals directory there");

        // the usual way to put journals on their own disk: link the Journals directory
        string journalsLink = Path.Combine(dataPath, "Journals");
        Directory.CreateDirectory(journalsTarget);
        using (var mklink = Process.Start(new ProcessStartInfo("cmd.exe", $"/c mklink /J \"{journalsLink}\" \"{journalsTarget}\"") { UseShellExecute = false, CreateNoWindow = true }))
        {
            mklink.WaitForExit();
            Assert.Equal(0, mklink.ExitCode);
        }

        try
        {
            using var server = GetNewServer();
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, Path = dataPath }))
            {
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

                // a slow journal write is on the other disk: no evidence for this one
                database.CheckWriteRateAndNotifyIfNecessary(SlowJournalWrite(database));
                Assert.Empty(monitor.GetTracker(device).GetSlowWrites(now));

                // the journal latency of this device comes from the linked disk, so the status names it
                Assert.Contains($"journals on {Path.GetPathRoot(journalsTarget)}", StorageStatusInfo.Create(monitor).Summary);
            }
        }
        finally
        {
            Directory.Delete(journalsLink); // the link, not its target
            Directory.Delete(journalsTarget, recursive: true);
        }
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
