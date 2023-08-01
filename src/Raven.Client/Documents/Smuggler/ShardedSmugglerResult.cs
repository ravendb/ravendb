using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler;

public sealed class ShardedSmugglerResult : IShardedOperationResult<ShardNodeSmugglerResult>
{
    public List<ShardNodeSmugglerResult> Results { get; set; }

    public ShardedSmugglerResult()
    {
        Message = null;
    }

    public string Message { get; private set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()))
        };
    }

    public bool ShouldPersist => true;
    public bool CanMerge => false;
    public void MergeWith(IOperationResult result)
    {
        throw new NotImplementedException();
    }

    public void CombineWith(IOperationResult result, int shardNumber, string nodeTag)
    {
        Results ??= new List<ShardNodeSmugglerResult>();

        if (result is not SmugglerResult sr)
            return;

        Results.Add(new ShardNodeSmugglerResult
        {
            Result = sr,
            ShardNumber = shardNumber,
            NodeTag = nodeTag
        });
    }
}

public sealed class ShardNodeSmugglerResult : ShardNodeOperationResult<SmugglerResult>
{
    public override bool ShouldPersist => true;
}
