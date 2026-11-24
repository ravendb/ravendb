using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Backups;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup;

// Code review repros for PR 22825 (centralized backup runner) at cbe7df4.
// Every test asserts the expected behavior, so it fails while the defect exists.
public class ServerBackupRunnerCodeReviewRepros : ClusterTestBase
{
    public ServerBackupRunnerCodeReviewRepros(ITestOutputHelper output) : base(output)
    {
    }

    // A database that already holds a concurrent backup place may start more backups (ConcurrentBackupsCounter.CanRunBackup returns true for it,
    // "allow to backup all shards of the same database concurrently"). The runner checks that server policy only against the task at the head
    // of the queue and ends the pass when it fails, so a due backup of that database is never reached while another database's task heads the queue.
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public Task DueBackupOfADatabaseThatAlreadyBacksUpMustStartWhateverTheQueueOrder() => DueBackupOfADatabaseThatAlreadyBacksUpMustStart(withOtherDatabaseAtTheHead: true);

    // control: same scenario without another database's task at the head of the queue, passes on cbe7df4
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public Task DueBackupOfADatabaseThatAlreadyBacksUpMustStart_Control() => DueBackupOfADatabaseThatAlreadyBacksUpMustStart(withOtherDatabaseAtTheHead: false);

    private async Task DueBackupOfADatabaseThatAlreadyBacksUpMustStart(bool withOtherDatabaseAtTheHead)
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1",
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });

        var runner = server.ServerStore.ServerBackupRunner;
        var counter = server.ServerStore.ConcurrentBackupsCounter;

        // database Y: its task is registered first, so it heads the queue; it is not due during the test
        using var storeY = GetDocumentStore(new Options { Server = server });
        if (withOtherDatabaseAtTheHead)
        {
            await StoreDocument(storeY);
            var taskY = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolderY"), fullBackupFrequency: "0 0 1 1 *"), storeY);
            await WaitForStateAsync(runner, storeY.Database, taskY);
            Backup.WaitForResponsibleNodeUpdate(server.ServerStore, storeY.Database, taskY);
        }

        // database X: task 1 runs (Backup Now, held), task 2 becomes due at the next minute boundary
        using var storeX = GetDocumentStore(new Options { Server = server });
        await StoreDocument(storeX);
        var databaseX = await Databases.GetDocumentDatabaseInstanceFor(server, storeX);

        await WaitForSecondOfMinuteBelow(40);

        var task1 = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolderX1"), fullBackupFrequency: "0 0 1 1 *"), storeX);
        var task2 = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolderX2"), fullBackupFrequency: "* * * * *"), storeX);
        await WaitForStateAsync(runner, storeX.Database, task1);
        var state2 = await WaitForStateAsync(runner, storeX.Database, task2);
        Assert.Equal(server.ServerStore.NodeTag, Backup.WaitForResponsibleNodeUpdate(server.ServerStore, storeX.Database, task1));
        Assert.Equal(server.ServerStore.NodeTag, Backup.WaitForResponsibleNodeUpdate(server.ServerStore, storeX.Database, task2));
        Assert.True(WaitForValue(() => state2.NextBackup != null, true), "precondition: task 2 is scheduled");
        var dueUtc = state2.NextBackup.DateTime;

        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[storeX.Database] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            runner.StartBackupNowTask(databaseX, task1, isFullBackup: true, databaseX.Operations.GetNextOperationId());

            Assert.True(counter.CanRunBackup(storeX.Database), "precondition: database X may start more backups while it already backs up");
            Assert.False(counter.CanRunBackup(storeY.Database), "precondition: the only concurrent backup place is taken");
            Assert.True(dueUtc > DateTime.UtcNow, "precondition: task 2 becomes due only after task 1 holds the place");

            var waitUntil = dueUtc + TimeSpan.FromSeconds(15);
            if (waitUntil > DateTime.UtcNow)
                await Task.Delay(waitUntil - DateTime.UtcNow);

            var decisions2 = BackupTestBase.GetDecisionLog(runner, storeX.Database, task2) ?? new List<BackupDecision>();
            var started = decisions2.Any(x => x.Category == BackupDecisionCategory.Started);
            var serverRejections = runner.Decisions.GetDecisionLogDetails(new BackupDecisionLogFilter { Take = 50 }, afterEtag: null).Entries
                .Where(x => x.Source == BackupDecisionSource.Server && x.Detail == "ConcurrentBackup")
                .Sum(x => x.Count);

            Assert.True(started,
                $"Task 2 of '{storeX.Database}' was due at {dueUtc:O} and its database already holds a concurrent backup place " +
                $"(CanRunBackup('{storeX.Database}') == true), but it never started. The runner logged {serverRejections} 'ConcurrentBackup' server rejections " +
                $"for the head of the queue (task of '{storeY.Database}'). Newest decision of task 2: " +
                $"{decisions2.Select(d => $"{d.LastTime:HH:mm:ss} x{d.Count} {d.Category} {d.Detail} {d.Reason}").FirstOrDefault() ?? "none"}");
        }
        finally
        {
            hold.TrySetResult(null);
        }
    }

    // Every DatabaseBackupState pre-allocates a BackupDecision[1024] ring buffer. Tracked states (one per task, also on non-responsible nodes)
    // and the throwaway states AbstractDatabasesHandlerProcessorForAllowedDatabases.GetBackupInfo builds per backup task of every unloaded database
    // on each /databases request all pay ~8 KB up front.
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public void DatabaseBackupStateMustNotPreallocateItsWholeDecisionLog()
    {
        GC.KeepAlive(new BackupDecisionLog(BackupDecisionTracker.MaxDecisionLogSize)); // JIT warm-up

        var before = GC.GetAllocatedBytesForCurrentThread();
        var decisionLog = new BackupDecisionLog(BackupDecisionTracker.MaxDecisionLogSize);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(decisionLog);

        Assert.True(allocated < 2048,
            $"Creating an empty BackupDecisionLog allocated {allocated:N0} bytes (MaxDecisionLogSize = {BackupDecisionTracker.MaxDecisionLogSize}).");
    }

    // WaitHandle.WaitAny(WaitHandle[], TimeSpan) throws ArgumentOutOfRangeException for a timeout above Int32.MaxValue ms (~24.86 days).
    // GetWaitBeforeNextRun returns BackupRunnerFrequency (or a CPU credits / low memory hold) unclamped, so the exception escapes to the outer
    // catch of Run and the runner stops for good. The old runner clamped its timers to MaxTimerTimeout.
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task RunnerMustKeepRunningWhenItsWaitIsLongerThan24Days()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                // 25 days
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = (25 * 24 * 3600).ToString()
            }
        });

        var id = AlertRaised.GetKey(AlertReason.PeriodicBackup, nameof(ServerBackupRunner));
        string title = null;
        string exception = null;

        await WaitForValueAsync(() =>
        {
            using (server.ServerStore.NotificationCenter.Storage.Read(id, out var stored))
            {
                if (stored == null)
                    return Task.FromResult(false);

                stored.Json.TryGet(nameof(AlertRaised.Title), out title);
                if (stored.Json.TryGet(nameof(AlertRaised.Details), out Sparrow.Json.BlittableJsonReaderObject details))
                    details.TryGet("Exception", out exception);

                return Task.FromResult(true);
            }
        }, true, timeout: 10_000, interval: 250);

        Assert.True(title != "Server Backup Runner",
            $"The server backup runner stopped with BackupRunnerFrequencyInSec of 25 days: '{title}'. " +
            $"{exception?.Split(Environment.NewLine).FirstOrDefault()}");
    }

    // The 'runner stopped' alert passes _serverStore.NodeTag as the 'database' argument of AlertRaised.Create.
    // Stops the runner through the BeforeRunnerPass test hook (the 25-day setting no longer stops it since the wait is capped), then reads the stored alert.
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task RunnerStoppedAlertMustNotBeAttributedToADatabase()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });

        server.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().BeforeRunnerPass = () => throw new InvalidOperationException("Simulated runner failure");

        var id = AlertRaised.GetKey(AlertReason.PeriodicBackup, nameof(ServerBackupRunner));
        string title = null;
        string database = null;

        Assert.True(await WaitForValueAsync(() =>
        {
            using (server.ServerStore.NotificationCenter.Storage.Read(id, out var stored))
            {
                if (stored == null)
                    return Task.FromResult(false);

                stored.Json.TryGet(nameof(AlertRaised.Title), out title);
                stored.Json.TryGet(nameof(AlertRaised.Database), out database);
                return Task.FromResult(true);
            }
        }, true, timeout: 10_000, interval: 250), "precondition: the runner stopped and raised its alert");

        Assert.Equal("Server Backup Runner", title);
        Assert.True(database == null,
            $"The server-level '{title}' alert is attributed to database '{database}' (node tag '{server.ServerStore.NodeTag}').");
    }

    private static async Task StoreDocument(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
            await session.SaveChangesAsync();
        }
    }

    private async Task<DatabaseBackupState> WaitForStateAsync(ServerBackupRunner runner, string database, long taskId)
    {
        DatabaseBackupState state = null;

        Assert.True(await WaitForValueAsync(() => Task.FromResult((state = BackupTestBase.GetBackupState(runner, database, taskId)) != null), true, timeout: 15_000, interval: 100),
            $"precondition: runner should track backup task {taskId} of '{database}'");

        return state;
    }

    private static async Task WaitForSecondOfMinuteBelow(int second)
    {
        while (DateTime.Now.Second >= second)
            await Task.Delay(250);
    }
}
