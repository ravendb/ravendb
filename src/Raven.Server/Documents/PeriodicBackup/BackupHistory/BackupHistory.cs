using System.Collections.Generic;
using System.Linq;
using Raven.Server.ServerWide;
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
    
    private readonly Dictionary<(long TaskId, int? ShardNumber, long FullBackupTicks), BackupGroup> _groupsByKey = [];

    public void Add(BackupHistoryEntry entry, long taskId, int? shardNumber, bool includeIncrementals)
    {
        var key = (taskId, shardNumber, entry.GetFullBackupTicks());
        if (_groupsByKey.TryGetValue(key, out var group) == false)
        {
            group = new BackupGroup(entry, taskId, shardNumber);
            _groupsByKey.Add(key, group);
            Groups.Add(group);
        }

        if (entry.BackupKind == BackupKind.Full)
            group.FullBackup = entry;
        else if (includeIncrementals)
            group.AddIncrementalBackup(entry);
        else
            group.IncrementalBackupsCount++;
    }
    
    public void UpdateTaskNames(RawDatabaseRecord databaseRecord)
    {
        foreach (var backupGroup in Groups)
        {
            var taskNameFromDbRecord = databaseRecord.PeriodicBackups.FirstOrDefault(x => x.TaskId == backupGroup.TaskId)?.Name;
            if (taskNameFromDbRecord == null)
            {
                // task was removed from the database record
                // we keep the existing task name in the backup group
                continue;
            }

            // the task name was changed, we need to update the backup group
            if (backupGroup.TaskName != taskNameFromDbRecord)
                backupGroup.TaskName = taskNameFromDbRecord;
        }
    }

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(Groups)] = 
                new DynamicJsonArray(collection: Groups.Select(group => group.ToJson()))
        };

    internal string ToString(TransactionContextPool contextPool)
    {
        using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            return ctx.ReadObject(ToJson(), nameof(BackupHistory)).ToString();
        }
    }
}
