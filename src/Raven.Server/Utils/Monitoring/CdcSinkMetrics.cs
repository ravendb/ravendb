using Raven.Server.Documents.TasksErrors;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring;

public sealed class CdcSinkMetrics
{
    public string ProcessName { get; set; }
    public long ErrorsCount { get; set; }
    public OngoingTaskHealthStatus HealthStatus { get; set; }
    public double? LastSuccessfulBatchTimeInSec { get; set; }

    public CdcSinkMetrics()
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

public sealed class CdcSinksMetrics
{
    public string PublicServerUrl { get; set; }
    public string NodeTag { get; set; }
    public List<PerDatabaseCdcSinkMetrics> Results { get; set; } = new List<PerDatabaseCdcSinkMetrics>();

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

public sealed class PerDatabaseCdcSinkMetrics
{
    public string DatabaseName { get; set; }
    public List<CdcSinkMetrics> CdcSinks { get; set; } = new List<CdcSinkMetrics>();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(CdcSinks)] = CdcSinks.Select(x => x.ToJson()).ToList()
        };
    }
}
