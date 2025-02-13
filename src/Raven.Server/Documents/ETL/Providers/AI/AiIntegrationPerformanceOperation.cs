using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiIntegrationPerformanceOperation : EtlPerformanceOperation
{
    public AiIntegrationPerformanceOperation(TimeSpan duration)
        : base(duration)
    {
    }
    
    public TimeSpan GenerateEmbeddings { get; set; }
}
