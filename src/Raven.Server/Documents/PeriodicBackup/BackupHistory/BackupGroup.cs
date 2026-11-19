using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupGroup
{
    public BackupGroup() { }

    public BackupGroup(long taskId)
    {
        TaskId = taskId;
    }
    
    public BackupGroup(BackupHistoryEntry entry, long taskId, int? shardNumber, string taskName)
    {
        TaskId = taskId;
        ShardNumber = shardNumber;
        TaskName = taskName;

        switch (entry.BackupKind)
        {
            case BackupKind.Full:
                FullBackup = entry;
                FullBackupInHistory = true;
                _incrementalBackups = [];
                break;
            
            case BackupKind.Incremental:
                FullBackup = new BackupHistoryEntry
                {
                    CreatedAt = entry.LastFullBackup ?? DateTime.MinValue,
                    DurationInMs = null,
                    Error = null,
                    BackupKind = BackupKind.Full,
                    LastFullBackup = entry.LastFullBackup,
                };

                break;
        }
    }
    
    public long TaskId { get; init; }
    public int? ShardNumber { get; init; }
    public string TaskName { get; set; }
    public BackupHistoryEntry FullBackup { get; set; }
    public bool FullBackupInHistory { get; set; }
    public long IncrementalBackupsCount { get; set; }
    private readonly List<BackupHistoryEntry> _incrementalBackups = [];
    public IReadOnlyList<BackupHistoryEntry> IncrementalBackups
    {
        get => _incrementalBackups;
        private init => _incrementalBackups.AddRange(value ?? []);
    }

    public void AddIncrementalBackup(BackupHistoryEntry entry)
    {
        Debug.Assert(entry.BackupKind == BackupKind.Incremental);

        _incrementalBackups.Add(entry);
        IncrementalBackupsCount++;
    }
    
    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName,
            [nameof(FullBackup)] = FullBackup,
            [nameof(FullBackupInHistory)] = FullBackupInHistory,
            [nameof(IncrementalBackupsCount)] = IncrementalBackupsCount,
        };

        if (ShardNumber.HasValue)
            json[nameof(ShardNumber)] = ShardNumber;

        if (IncrementalBackups.Count > 0)
            json[nameof(IncrementalBackups)] = IncrementalBackups;

        return json;
    }
}
