using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring;

public sealed class AiTaskMetrics
{
    public string ProcessName { get; set; }
    public long ErrorsCount { get; set; }
    public EtlProcessHealthStatus HealthStatus { get; set; }
    public double? LastSuccessfulBatchTimeInSec { get; set; }
    public double DocumentsProcessedPerSec { get; set; }

    public AiTaskMetrics()
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
            [nameof(LastSuccessfulBatchTimeInSec)] = LastSuccessfulBatchTimeInSec,
            [nameof(DocumentsProcessedPerSec)] = DocumentsProcessedPerSec
        };
    }
}

public sealed class AiTasksMetrics
{
    public string PublicServerUrl { get; set; }
    public string NodeTag { get; set; }
    public List<PerDatabaseAiTaskMetrics> Results { get; set; } = new List<PerDatabaseAiTaskMetrics>();

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

public sealed class PerDatabaseAiTaskMetrics
{
    public string DatabaseName { get; set; }
    public List<AiTaskMetrics> AiTasks { get; set; } = new List<AiTaskMetrics>();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(AiTasks)] = AiTasks.Select(x => x.ToJson()).ToList()
        };
    }
}

