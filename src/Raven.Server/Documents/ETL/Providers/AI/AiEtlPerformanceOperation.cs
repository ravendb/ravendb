using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtlPerformanceOperation : EtlPerformanceOperation
{
    public AiEtlPerformanceOperation(TimeSpan duration)
        : base(duration)
    {
    }
    
    public TimeSpan GenerateEmbeddings { get; set; }
}
