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

    public void ApplyPaging(int start, int pageSize)
    {
        TotalResults = Groups.Count;

        var page = Groups
            .OrderByDescending(group => group.FullBackup.CreatedAt)
            .ThenBy(group => group.TaskId)
            .ThenBy(group => group.ShardNumber)
            .Skip(start)
            .Take(pageSize)
            .ToList();

        Groups.Clear();
        Groups.AddRange(page);
    }

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
