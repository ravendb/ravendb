using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed class BackupHistoryCounts
{
    public long Total { get; set; }
    public Dictionary<string, long> ByTrigger { get; set; } = [];
    public Dictionary<string, long> ByBackupType { get; set; } = [];

    internal void Add(BackupHistoryEntry entry)
    {
        Total++;
        if (entry.Trigger.HasValue)
            Increment(ByTrigger, entry.Trigger.Value.ToString());

        Increment(ByBackupType, entry.BackupType.ToString());
    }

    private static void Increment(Dictionary<string, long> counts, string key)
    {
        counts[key] = counts.GetValueOrDefault(key) + 1;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Total)] = Total,
            [nameof(ByTrigger)] = ToJson(ByTrigger),
            [nameof(ByBackupType)] = ToJson(ByBackupType)
        };
    }

    private static DynamicJsonValue ToJson(Dictionary<string, long> counts)
    {
        var json = new DynamicJsonValue();
        foreach (var (key, count) in counts)
            json[key] = count;

        return json;
    }
}
