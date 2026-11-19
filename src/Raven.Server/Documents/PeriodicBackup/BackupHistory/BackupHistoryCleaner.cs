using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed class BackupHistoryCleaner : BackgroundWorkBase
{
    private const int MaxEntriesToDeletePerCommand = 1024;

    private readonly ServerStore _serverStore;

    public BackupHistoryCleaner(ServerStore serverStore)
        : base(nameof(BackupHistoryCleaner), RavenLogManager.Instance.GetLoggerForServer<BackupHistoryCleaner>(), serverStore.ServerShutdown)
    {
        _serverStore = serverStore;
    }

    protected override async Task DoWork()
    {
        await CleanupAsync(DateTime.UtcNow);

        await WaitOrThrowOperationCanceled(_serverStore.Configuration.Backup.BackupHistoryCleanupInterval.AsTimeSpan);
    }

    internal long DeleteDatabaseHistory(string databaseName) =>
        AsyncHelpers.RunSync(() => DeleteExpiredEntriesAsync(new DatabaseToClean(databaseName, DateTime.MaxValue, [])));

    internal async Task<long> CleanupAsync(DateTime utcNow)
    {
        var deleted = 0L;

        foreach (var database in GetDatabasesToClean(utcNow))
        {
            CancellationToken.ThrowIfCancellationRequested();

            try
            {
                var deletedFromDatabase = await DeleteExpiredEntriesAsync(database);
                deleted += deletedFromDatabase;

                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Removed {deletedFromDatabase} backup history entries of '{database.DatabaseName}' that are older than {database.CutoffTime:O}.");
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                if (Logger.IsWarnEnabled)
                    Logger.Warn($"Failed to remove the expired backup history of '{database.DatabaseName}'.", e);
            }
        }

        return deleted;
    }

    private async Task<long> DeleteExpiredEntriesAsync(DatabaseToClean database)
    {
        var maxEntriesToDelete = _forTestingPurposes?.MaxEntriesToDeletePerCommand ?? MaxEntriesToDeletePerCommand;
        var deleted = 0L;

        while (true)
        {
            var command = new DeleteExpiredBackupHistoryCommand(database.DatabaseName, database.CutoffTime, database.TasksToKeepNewestFullBackup, maxEntriesToDelete);
            await _serverStore.Engine.TxMerger.Enqueue(command);

            deleted += command.DeletedEntries;
            if (command.DeletedEntries < maxEntriesToDelete)
                return deleted;
        }
    }

    private List<DatabaseToClean> GetDatabasesToClean(DateTime utcNow)
    {
        var databasesToClean = new List<DatabaseToClean>();

        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var databaseName in BackupHistoryStorage.GetDatabaseNames(context))
            {
                try
                {
                    using (var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardHelper.ToDatabaseName(databaseName)))
                    {
                        var topology = GetTopology(databaseName, databaseRecord);
                        var cutoffTime = topology == null ? DateTime.MaxValue : GetCutoffTime(utcNow, GetRetentionPeriod(databaseRecord));

                        if (BackupHistoryStorage.HasExpiredEntries(context, databaseName, cutoffTime) == false)
                            continue;

                        var tasksToKeepNewestFullBackup = topology?.RelevantFor(_serverStore.NodeTag) == true
                            ? databaseRecord.PeriodicBackupsTaskIds.ToHashSet()
                            : [];

                        databasesToClean.Add(new DatabaseToClean(databaseName, cutoffTime, tasksToKeepNewestFullBackup));
                    }
                }
                catch (Exception e)
                {
                    if (Logger.IsWarnEnabled)
                        Logger.Warn($"Failed to read the backup history retention period of '{databaseName}', its expired backup history is not removed.", e);
                }
            }
        }

        return databasesToClean;
    }

    private static DateTime GetCutoffTime(DateTime utcNow, TimeSpan retentionPeriod) =>
        retentionPeriod < utcNow - DateTime.MinValue ? utcNow - retentionPeriod : DateTime.MinValue;

    private static DatabaseTopology GetTopology(string databaseName, RawDatabaseRecord databaseRecord)
    {
        if (databaseRecord == null)
            return null;

        if (ShardHelper.TryGetShardNumberAndDatabaseName(databaseName, out _, out var shardNumber) == false)
            return databaseRecord.Topology;

        return databaseRecord.IsSharded && databaseRecord.Sharding.Shards.TryGetValue(shardNumber, out var shardTopology) ? shardTopology : null;
    }

    private TimeSpan GetRetentionPeriod(RawDatabaseRecord databaseRecord)
    {
        var retentionPeriod = _forTestingPurposes?.RetentionPeriod;
        if (retentionPeriod.HasValue)
            return retentionPeriod.Value;

        return DatabasesLandlord.CreateDatabaseConfiguration(_serverStore, databaseRecord.DatabaseName, databaseRecord.Settings).Backup.BackupHistoryRetentionPeriod.AsTimeSpan;
    }

    private readonly record struct DatabaseToClean(string DatabaseName, DateTime CutoffTime, HashSet<long> TasksToKeepNewestFullBackup);

    private TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly() => _forTestingPurposes ??= new TestingStuff();

    internal sealed class TestingStuff
    {
        internal int? MaxEntriesToDeletePerCommand { get; set; }
        internal TimeSpan? RetentionPeriod { get; set; }
    }
}
