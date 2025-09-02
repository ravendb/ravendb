using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupGroup
{
    public BackupGroup() { }
    
    public BackupGroup(BackupHistoryEntry entry, long taskId)
    {
        TaskId = taskId;

        switch (entry.BackupKind)
        {
            case BackupKind.Full:
                FullBackup = entry;
                _incrementalBackups = [];
                break;
            
            case BackupKind.Incremental:
                FullBackup = new BackupHistoryEntry
                {
                    CreatedAt = entry.LastFullBackup ?? DateTime.MinValue,
                    DurationInMs = null,
                    Error = null,
                    BackupKind = BackupKind.Full,
                    NodeTag = entry.NodeTag,
                    LastFullBackup = entry.LastFullBackup,
                };
                
                AddIncrementalBackup(entry);

                break;
        }
    }
    
    public long TaskId { get; init; }
    public string TaskName { get; set; }
    public BackupHistoryEntry FullBackup { get; set; }
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
            [nameof(IncrementalBackupsCount)] = IncrementalBackupsCount,
        };

        if (IncrementalBackups.Count > 0)
            json[nameof(IncrementalBackups)] = IncrementalBackups;

        return json;
    }
}
