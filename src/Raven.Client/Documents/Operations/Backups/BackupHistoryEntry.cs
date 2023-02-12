using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups;

public class BackupHistoryEntry : IDynamicJsonValueConvertible
{
    public string GenerateItemKey() => $"values/{DatabaseName}/backup-history/{TaskId}";
    public static string GenerateItemPrefix(string databaseName) => $"values/{databaseName}/backup-history/";

    public BackupType BackupType { get; set; }
    public DateTime CreatedAt { get; set; }
    public string DatabaseName { get; set; }
    public long? DurationInMs { get; set; }
    public string Error { get; set; }
    public bool IsFull { get; set; }
    public string NodeTag { get; set; }
    public DateTime? LastFullBackup { get; set; }
    public long TaskId { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(BackupType)] = BackupType,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(DurationInMs)] = DurationInMs,
            [nameof(Error)] = Error,
            [nameof(IsFull)] = IsFull,
            [nameof(NodeTag)] = NodeTag,
            [nameof(LastFullBackup)] = LastFullBackup,
            [nameof(TaskId)] = TaskId,
        };
    }
}

public class BackupHistory
{
    public BlittableJsonReaderObject FullBackup;
    public BlittableJsonReaderArray IncrementalBackups;
}
