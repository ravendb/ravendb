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

        public DynamicJsonValue ToJson()
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
                [nameof(Gen0HeapSize)] = Gen0HeapSize.ToJson(),
                [nameof(Gen1HeapSize)] = Gen1HeapSize.ToJson(),
                [nameof(Gen2HeapSize)] = Gen2HeapSize.ToJson(),
                [nameof(LargeObjectHeapSize)] = LargeObjectHeapSize.ToJson(),
                [nameof(PinnedObjectHeapSize)] = PinnedObjectHeapSize.ToJson(),
            };
        }
    }

    public class GenerationInfoSize : IDynamicJson
    {
        public long SizeBeforeBytes { get; set; }

        public long SizeAfterBytes { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SizeBeforeBytes)] = SizeBeforeBytes,
                [nameof(SizeAfterBytes)] = SizeAfterBytes,
            };
        }
    }
}
