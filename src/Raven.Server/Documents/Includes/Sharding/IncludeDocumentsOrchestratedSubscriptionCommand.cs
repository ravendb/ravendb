using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class IncludeDocumentsOrchestratedSubscriptionCommand : AbstractIncludeDocumentsCommand
{
    private readonly JsonOperationContext _clusterContext;
    private readonly CancellationToken _token;

    public List<BlittableJsonReaderObject> Includes { get; }

    public override int Count => Includes.Count;

    public IncludeDocumentsOrchestratedSubscriptionCommand(JsonOperationContext clusterContext, CancellationToken token)
    {
        Includes = new List<BlittableJsonReaderObject>();
        _clusterContext = clusterContext;
        _token = token;
    }

    public void Gather(List<BlittableJsonReaderObject> includes)
    {
        if (includes == null || includes.Count == 0)
            return;

        foreach (var include in includes)
        {
            _token.ThrowIfCancellationRequested();
            
            if (include != null)
            {
                using (include)
                {
                    Includes.Add(include.Clone(_clusterContext));
                }
            }
            else
            {
                Debug.Assert(false, "we handled missing includes in ShardedSubscriptionBatch.Initialize() there should be no nulls");
            }
        }
    }
}
