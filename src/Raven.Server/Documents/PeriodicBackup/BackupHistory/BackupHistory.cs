using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

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
                var groupToAdd = Groups.FirstOrDefault(group => group.FullBackup?.LastFullBackup == entry.LastFullBackup && group.TaskId == taskId);
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
    
    public void UpdateTaskNames(DatabaseRecord databaseRecord, Logger logger)
    {
        foreach (var backupGroup in Groups)
        {
            var taskNameFromDbRecord = databaseRecord.PeriodicBackups.FirstOrDefault(x => x.TaskId == backupGroup.TaskId)?.Name;
            if (taskNameFromDbRecord == null)
            {
                // task was removed from the database record
                if (backupGroup.TaskName == null)
                {
                    // we don't have a task name in the backup group, this is an inconsistent state
                    if (logger.IsInfoEnabled)
                        logger.Info($"Task name for backup group with task ID '{backupGroup.TaskId}' could not be determined. " +
                                  $"It was not found in the database record for '{DatabaseName}' and was not previously set in the backup history. " +
                                  "Assigning a placeholder name '<Unknown Task>'.");

                    backupGroup.TaskName = "<Unknown Task>";
                }

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
