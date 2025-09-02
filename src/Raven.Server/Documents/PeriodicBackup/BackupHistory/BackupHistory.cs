using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
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
    
    public void Add(BackupHistoryEntry entry, long taskId)
    {
        switch (entry.BackupKind)
        {
            case BackupKind.Full:
                var existingGroup = Groups.FirstOrDefault(group => group.FullBackup?.CreatedAt == entry.CreatedAt && group.TaskId == taskId);
                if (existingGroup == null)
                    Groups.Add(new BackupGroup(entry, taskId));
                else
                    existingGroup.FullBackup = entry;
                break;
            
            case BackupKind.Incremental:
                var groupToAdd = Groups.FirstOrDefault(group => group.FullBackup?.LastFullBackup == entry.LastFullBackup);
                if (groupToAdd == null)
                {
                    groupToAdd = new BackupGroup(entry, taskId);
                    Groups.Add(groupToAdd);
                }
                else
                {
                    groupToAdd.AddIncrementalBackup(entry);
                }
                break;
        }
    }
    
    public void UpdateTaskNames(DatabaseRecord databaseRecord)
    {
        foreach (var backupGroup in Groups)
        {
            var taskName = databaseRecord.PeriodicBackups.FirstOrDefault(x => x.TaskId == backupGroup.TaskId)?.Name;
            if (taskName != null && backupGroup.TaskName != taskName)
                // Task name was changed, we need to update the backup group
                backupGroup.TaskName = taskName;
            else if (backupGroup.TaskName == null && taskName == null)
                // Task name was not set, and we don't have a task name in the database record, shouldn't happen
                backupGroup.TaskName = "N/A";
            
            // Else, the task name is already set, and we don't need to update it
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
