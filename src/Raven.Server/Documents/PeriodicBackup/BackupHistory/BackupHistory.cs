using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupHistory
{
    // ReSharper disable once UnusedMember.Global
    public BackupHistory() { } // For deserialization

    public BackupHistory(string databaseName)
    {
        DatabaseName = databaseName;
    }
    
    public string DatabaseName { get; init; }
    public List<BackupGroup> Groups { get; init; } = [];
    public long TotalResults { get; set; }
    public BackupHistoryCounts Counts { get; init; } = new();
    
    private readonly Dictionary<(long TaskId, int? ShardNumber, long FullBackupTicks), BackupGroup> _groupsByKey = [];

    public void Add(BackupHistoryEntry entry, long taskId, int? shardNumber, string taskName, bool includeIncrementals)
    {
        var key = (taskId, shardNumber, entry.GetFullBackupTicks());
        if (_groupsByKey.TryGetValue(key, out var group) == false)
        {
            group = new BackupGroup(entry, taskId, shardNumber, taskName);
            _groupsByKey.Add(key, group);
            Groups.Add(group);
        }

        if (entry.BackupKind == BackupKind.Full)
        {
            group.FullBackup = entry;
            group.FullBackupInHistory = true;
        }
        else if (includeIncrementals)
        {
            group.AddIncrementalBackup(entry);
        }
        else
        {
            group.IncrementalBackupsCount++;
        }
    }

    public void SortAndPage(BackupHistoryQuery query)
    {
        TotalResults = Groups.Count;

        var page = Sort(Groups, query, group => group.FullBackup, group => group.TaskName, group => group.TaskId, group => group.ShardNumber)
            .Skip(query.Start)
            .Take(query.PageSize)
            .ToList();

        Groups.Clear();
        Groups.AddRange(page);
    }

    private static IEnumerable<T> Sort<T>(IEnumerable<T> items, BackupHistoryQuery query, Func<T, BackupHistoryEntry> getEntry, Func<T, string> getTaskName,
        Func<T, long> getTaskId, Func<T, int?> getShardNumber)
    {
        var ordered = query.SortBy switch
        {
            BackupHistorySortBy.TaskName => OrderBy(items, getTaskName, StringComparer.OrdinalIgnoreCase, query.Descending),
            BackupHistorySortBy.Node => OrderBy(items, item => getEntry(item).NodeTag, StringComparer.Ordinal, query.Descending),
            BackupHistorySortBy.Duration => OrderBy(items, item => getEntry(item).DurationInMs, Comparer<long?>.Default, query.Descending),
            _ => OrderBy(items, item => getEntry(item).CreatedAt, Comparer<DateTime>.Default, query.Descending)
        };

        return ordered
            .ThenByDescending(item => getEntry(item).CreatedAt)
            .ThenBy(getTaskId)
            .ThenBy(getShardNumber);
    }

    private static IOrderedEnumerable<T> OrderBy<T, TKey>(IEnumerable<T> items, Func<T, TKey> getKey, IComparer<TKey> comparer, bool descending) =>
        descending ? items.OrderByDescending(getKey, comparer) : items.OrderBy(getKey, comparer);

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(Groups)] = 
                new DynamicJsonArray(collection: Groups.Select(group => group.ToJson())),
            [nameof(TotalResults)] = TotalResults,
            [nameof(Counts)] = Counts.ToJson()
        };

    internal string ToString(TransactionContextPool contextPool)
    {
        using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            return ctx.ReadObject(ToJson(), nameof(BackupHistory)).ToString();
        }
    }
}
