using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications;

public class GcInfoPayload : AbstractClusterDashboardNotification
{
    public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.GcInfo;

    public GcMemoryInfo Ephemeral { get; set; }

    public GcMemoryInfo Background { get; set; }

    public GcMemoryInfo FullBlocking { get; set; }


    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Ephemeral)] = Ephemeral?.ToJson();
        json[nameof(Background)] = Background?.ToJson();
        json[nameof(FullBlocking)] = FullBlocking?.ToJson();

        return json;
    }

    public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
    {
        return ToJson();
    }

    public class GcMemoryInfo : IDynamicJson
    {
        public long Index { get; set; }

        public bool Compacted { get; set; }

        public bool Concurrent { get; set; }
        
        public int Generation {get; set; }

        public double PauseTimePercentage { get; set; }

        public List<double> PauseDurationsInMs { get; set; }

        public long TotalHeapSizeAfterBytes { get; set; }

        public GenerationInfoSize Gen0HeapSize { get; set; }

        public GenerationInfoSize Gen1HeapSize { get; set; }

        public GenerationInfoSize Gen2HeapSize { get; set; }

        public GenerationInfoSize LargeObjectHeapSize { get; set; }

        public GenerationInfoSize PinnedObjectHeapSize { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Index)] = Index,
                [nameof(Compacted)] = Compacted,
                [nameof(Concurrent)] = Concurrent,
                [nameof(Generation)] = Generation,
                [nameof(PauseTimePercentage)] = PauseTimePercentage,
                [nameof(PauseDurationsInMs)] = PauseDurationsInMs,
                [nameof(TotalHeapSizeAfterBytes)] = TotalHeapSizeAfterBytes,
                [nameof(Gen0HeapSize)] = Gen0HeapSize?.ToJson(),
                [nameof(Gen1HeapSize)] = Gen1HeapSize?.ToJson(),
                [nameof(Gen2HeapSize)] = Gen2HeapSize?.ToJson(),
                [nameof(LargeObjectHeapSize)] = LargeObjectHeapSize?.ToJson(),
                [nameof(PinnedObjectHeapSize)] = PinnedObjectHeapSize?.ToJson(),
            };
        }
    }

    public sealed class GcMemoryInfoMetrics : GcMemoryInfo
    {
        public long FinalizationPendingCount { get; set; }
        public long FragmentedInMb { get; set; }
        public long HeapSizeInMb { get; set; }
        public long HighMemoryLoadThresholdInMb { get; set; }
        public long MemoryLoadInMb { get; set; }
        public long PinnedObjectsCount { get; set; }
        public long PromotedInMb { get; set; }
        public long TotalAvailableMemoryInMb { get; set; }
        public long TotalCommittedInMb { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(FinalizationPendingCount)] = FinalizationPendingCount;
            djv[nameof(FragmentedInMb)] = FragmentedInMb;
            djv[nameof(HeapSizeInMb)] = HeapSizeInMb;
            djv[nameof(HighMemoryLoadThresholdInMb)] = HighMemoryLoadThresholdInMb;
            djv[nameof(MemoryLoadInMb)] = MemoryLoadInMb;
            djv[nameof(PinnedObjectsCount)] = PinnedObjectsCount;
            djv[nameof(PromotedInMb)] = PromotedInMb;
            djv[nameof(TotalAvailableMemoryInMb)] = TotalAvailableMemoryInMb;
            djv[nameof(TotalCommittedInMb)] = TotalCommittedInMb;

            return djv;
        }

        public double? GetPauseDurationSeconds(int index)
        {
            if (PauseDurationsInMs == null || PauseDurationsInMs.Count <= index)
                return null;

            return PauseDurationsInMs[index] / 1000d;
        }
    }

    public class GenerationInfoSize : IDynamicJson
    {
        public long SizeBeforeBytes { get; set; }

        public long SizeAfterBytes { get; set; }
        public long FragmentationBeforeBytes { get; set; }
        public long FragmentationAfterBytes { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SizeBeforeBytes)] = SizeBeforeBytes,
                [nameof(SizeAfterBytes)] = SizeAfterBytes,
                [nameof(FragmentationBeforeBytes)] = FragmentationBeforeBytes,
                [nameof(FragmentationAfterBytes)] = FragmentationAfterBytes,
            };
        }
    }
}
