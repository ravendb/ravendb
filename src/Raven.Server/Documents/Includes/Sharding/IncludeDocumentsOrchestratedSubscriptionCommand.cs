using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes.Sharding;

public class IncludeDocumentsOrchestratedSubscriptionCommand : IncludeDocumentsCommand
{
    private readonly ClusterOperationContext _clusterContext;
    private readonly CancellationToken _token;
    private List<BlittableJsonReaderObject> _includes;

    public IncludeDocumentsOrchestratedSubscriptionCommand(ClusterOperationContext clusterContext, CancellationToken token)
    {
        _clusterContext = clusterContext;
        _token = token;
    }

    internal override bool HasIncludesIds() => _includes is { Count: > 0 };

    public override void Fill(List<BlittableJsonReaderObject> result)
    {
        if (_includes == null)
            return;

        foreach (var res in _includes)
        {
            result.Add(res);
        }
    }

    public override void Gather(List<BlittableJsonReaderObject> includes)
    {
        if (includes == null || includes.Count == 0)
            return;

        _includes = new List<BlittableJsonReaderObject>();

        foreach (var include in includes)
        {
            _token.ThrowIfCancellationRequested();
       
            if (include != null)
            {
                using (include)
                {
                    _includes.Add(include.Clone(_clusterContext));
                }
            }
            else
            {
                // we handled missing includes in ShardedSubscriptionBatch.Initialize() there should be no nulls
                Debug.Assert(false);
            }
        }
    }

    public override void Gather(Document document)
    {
        // no-op, this is used by shard or regular database
    }

    public override void Fill(List<Document> result, bool includeMissingAsNull)
    {
        throw new NotImplementedException($"This method meant to be used in regular database or shard.");
    }
}
