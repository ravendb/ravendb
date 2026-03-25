using System;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;

public sealed class GenAiPerformanceOperation(TimeSpan duration) : EtlPerformanceOperation(duration)
{
    public int NumberOfContextObjects { get; set; }

    public int TotalSentToModel { get; set; }

    public int TotalCachedContexts { get; set; }

    public int ModelCallFailures { get; set; }

    public int TotalUpdates { get; set; }

    public int UpdateFailures { get; set; }

    public AiUsage Usage { get; set;  }
}
