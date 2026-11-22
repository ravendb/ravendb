using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed class BackupHistoryRun : BackupHistoryEntry
{
    public BackupHistoryRun() { }

    public BackupHistoryRun(BackupHistoryEntry entry, long taskId, int? shardNumber, string taskName) : base(entry)
    {
        TaskId = taskId;
        ShardNumber = shardNumber;
        TaskName = taskName;
    }

    public long TaskId { get; set; }
    public int? ShardNumber { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(TaskId)] = TaskId;

        if (ShardNumber.HasValue)
            json[nameof(ShardNumber)] = ShardNumber;

        return json;
    }
}
