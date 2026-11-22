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
            AddCount(ByTrigger, entry.Trigger.Value.ToString());

        AddCount(ByBackupType, entry.BackupType.ToString());
    }

    internal void Add(BackupHistoryCounts other)
    {
        Total += other.Total;

        AddCounts(ByTrigger, other.ByTrigger);
        AddCounts(ByBackupType, other.ByBackupType);
    }

    internal static void AddCount(Dictionary<string, long> counts, string key, long count = 1)
    {
        counts[key] = counts.GetValueOrDefault(key) + count;
    }

    internal static void AddCounts(Dictionary<string, long> counts, Dictionary<string, long> otherCounts)
    {
        foreach (var (key, count) in otherCounts)
            AddCount(counts, key, count);
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

    internal static DynamicJsonValue ToJson(Dictionary<string, long> counts)
    {
        var json = new DynamicJsonValue();
        foreach (var (key, count) in counts)
            json[key] = count;

        return json;
    }
}
