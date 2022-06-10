using System.Linq;
using Raven.Server.Documents.ETL.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats;

public class EtlTaskDebugStats : IDynamicJson
{
    public string TaskName { get; set; }

    public EtlProcessTransformationDebugStats[] Stats { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TaskName)] = TaskName,
            [nameof(Stats)] = new DynamicJsonArray(Stats.Select(x => x.ToJson()))
        };
    }
}

public class EtlProcessTransformationDebugStats : EtlProcessTransformationStats
{
    public EtlMetricsCountersManager Metrics { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Metrics)] = Metrics.ToJson();

        return json;
    }
}
