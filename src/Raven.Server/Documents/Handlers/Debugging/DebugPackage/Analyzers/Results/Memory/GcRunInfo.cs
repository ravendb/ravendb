using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class GcRunInfo : IDynamicJson
{
    public bool Compacted { get; set; }
    public bool Concurrent { get; set; }
    public int FinalizationPendingCount { get; set; }
    public long FragmentedBytes { get; set; }
    public string FragmentedHumane { get; set; }
    public int Generation { get; set; }
    public List<GenerationInfo> GenerationInfo { get; set; }
    public long HeapSizeBytes { get; set; }
    public string HeapSizeHumane { get; set; }
    public long HighMemoryLoadThresholdBytes { get; set; }
    public string HighMemoryLoadThresholdHumane { get; set; }
    public int Index { get; set; }
    public long MemoryLoadBytes { get; set; }
    public string MemoryLoadHumane { get; set; }
    public List<TimeSpan> PauseDurations { get; set; }
    public double PauseTimePercentage { get; set; }
    public int PinnedObjectsCount { get; set; }
    public long PromotedBytes { get; set; }
    public string PromotedHumane { get; set; }
    public long TotalAvailableMemoryBytes { get; set; }
    public string TotalAvailableMemoryHumane { get; set; }
    public long TotalCommittedBytes { get; set; }
    public string TotalCommittedHumane { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Compacted)] = Compacted,
            [nameof(Concurrent)] = Concurrent,
            [nameof(FinalizationPendingCount)] = FinalizationPendingCount,
            [nameof(FragmentedBytes)] = FragmentedBytes,
            [nameof(FragmentedHumane)] = FragmentedHumane,
            [nameof(Generation)] = Generation,
            [nameof(GenerationInfo)] = GenerationInfo?.Select(x => x.ToJson()).ToList() != null ? 
                new DynamicJsonArray(GenerationInfo.Select(x => x.ToJson())) : null,
            [nameof(HeapSizeBytes)] = HeapSizeBytes,
            [nameof(HeapSizeHumane)] = HeapSizeHumane,
            [nameof(HighMemoryLoadThresholdBytes)] = HighMemoryLoadThresholdBytes,
            [nameof(HighMemoryLoadThresholdHumane)] = HighMemoryLoadThresholdHumane,
            [nameof(Index)] = Index,
            [nameof(MemoryLoadBytes)] = MemoryLoadBytes,
            [nameof(MemoryLoadHumane)] = MemoryLoadHumane,
            [nameof(PauseDurations)] = PauseDurations != null ? new DynamicJsonArray(PauseDurations) : null,
            [nameof(PauseTimePercentage)] = PauseTimePercentage,
            [nameof(PinnedObjectsCount)] = PinnedObjectsCount,
            [nameof(PromotedBytes)] = PromotedBytes,
            [nameof(PromotedHumane)] = PromotedHumane,
            [nameof(TotalAvailableMemoryBytes)] = TotalAvailableMemoryBytes,
            [nameof(TotalAvailableMemoryHumane)] = TotalAvailableMemoryHumane,
            [nameof(TotalCommittedBytes)] = TotalCommittedBytes,
            [nameof(TotalCommittedHumane)] = TotalCommittedHumane
        };
    }
}
