using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring;

public sealed class EtlMetrics
{
    public string ProcessName { get; set; }
    public long ErrorsCount { get; set; }
    public EtlProcessHealthStatus HealthStatus { get; set; }
    public double? LastSuccessfulBatchTimeInSec { get; set; }
    public double DocumentsProcessedPerSec { get; set; }
    
    public EtlMetrics()
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

public sealed class EtlsMetrics
{
    public string PublicServerUrl { get; set; }
    public string NodeTag { get; set; }
    public List<PerDatabaseEtlMetrics> Results { get; set; } = new List<PerDatabaseEtlMetrics>();

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

public sealed class PerDatabaseEtlMetrics
{
    public string DatabaseName { get; set; }
    public List<EtlMetrics> Etls { get; set; } = new List<EtlMetrics>();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(Etls)] = Etls.Select(x => x.ToJson()).ToList()
        };
    }
}
