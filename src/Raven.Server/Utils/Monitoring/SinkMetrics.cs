using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring;

public sealed class SinkMetrics
{
    public string ProcessName { get; set; }
    public long ErrorsCount { get; set; }
    public EtlProcessHealthStatus HealthStatus { get; set; }
    public double? LastSuccessfulBatchTimeInSec { get; set; }

    public SinkMetrics()
    {
        // deserialization
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ProcessName)] = ProcessName,
            [nameof(ErrorsCount)] = ErrorsCount,
            [nameof(HealthStatus)] = HealthStatus,
            [nameof(LastSuccessfulBatchTimeInSec)] = LastSuccessfulBatchTimeInSec
        };
    }
}

public sealed class SinksMetrics
{
    public string PublicServerUrl { get; set; }
    public string NodeTag { get; set; }
    public List<PerDatabaseSinkMetrics> Results { get; set; } = new List<PerDatabaseSinkMetrics>();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PublicServerUrl)] = PublicServerUrl,
            [nameof(NodeTag)] = NodeTag,
            [nameof(Results)] = Results.Select(x => x.ToJson()).ToList()
        };
    }
}

public sealed class PerDatabaseSinkMetrics
{
    public string DatabaseName { get; set; }
    public List<SinkMetrics> Sinks { get; set; } = new List<SinkMetrics>();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(Sinks)] = Sinks.Select(x => x.ToJson()).ToList()
        };
    }
}
