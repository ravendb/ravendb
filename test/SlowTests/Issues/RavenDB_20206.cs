using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using SlowTests.Server.Documents.PeriodicBackup;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20206 : ClusterTestBase
{
    public RavenDB_20206(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_Not_Clear_Compare_Exchange_Tombstones_Of_A_Database_With_Identical_Prefix()
    {
        const string databaseName = "RavenDB_20206";

        var backupPath = NewDataPath(suffix: "BackupFolder");
        var diagnosticLogBuilder = new StringBuilder();
        var serverCreationOptions = new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
        };

        using var server = GetNewServer(serverCreationOptions);
        using var firstStore = GetDocumentStore(new Options { Server = server, ModifyDatabaseName = _ => databaseName });
        using var secondStore = GetDocumentStore(new Options { Server = server, ModifyDatabaseName = _ => $"{databaseName}_123"});

        server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.UtcNow:O}] {logLine}");
        server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
        Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

        // Create compare exchange values and tombstones on the first store
        await firstStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
        await firstStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
        await RavenDB_11139.CreateCompareExchangeTombstone(firstStore, "cx/3");

        // Create compare exchange values and tombstones on the second store
        await secondStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
        await secondStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
        await RavenDB_11139.CreateCompareExchangeTombstone(secondStore, "cx/3");

        RavenDB_11139.AssertCompareExchangeCounts(server, firstStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);
        RavenDB_11139.AssertCompareExchangeCounts(server, secondStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);

        var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration");
        var secondDatabaseBackupWaiter = new RavenDB_11139.NextBackupWaiter(clusterTestBase: this)
            .WithDatabase(secondStore.Database)
            .WithBackupConfiguration(backupConfiguration)
            .WithClusterNodes([server])
            .WithClusterObserverConfirmation()
            .WithDiagnosticLog(diagnosticLogBuilder)
            .SetMentorNodeTo(server, secondStore);

        await secondDatabaseBackupWaiter
            .TriggerNextOccurenceNowAsync(BackupKind.Full);

        await RavenDB_11139.CreateCompareExchangeTombstone(firstStore, "cx/4");
        await RavenDB_11139.CreateCompareExchangeTombstone(secondStore, "cx/4");

        // We only did a full backup on the second store, so the first store (without `_123` suffix) still has no backup configuration and can clean up compare exchange tombstones
        await Cluster.RunCompareExchangeTombstoneCleaner(clusterNodes: [server], ignoreClusterTrx: true);
        RavenDB_11139.AssertCompareExchangeCounts(server, firstStore.Database, expectedTombstonesNumber: 0, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup after full backup and compare exchange tombstone creation", diagnosticLogBuilder);
        RavenDB_11139.AssertCompareExchangeCounts(server, secondStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup (no backup configuration) and compare exchange tombstone creation", diagnosticLogBuilder);
    }
}
