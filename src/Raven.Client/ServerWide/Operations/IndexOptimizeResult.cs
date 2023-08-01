using System;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations;

public sealed class IndexOptimizeResult : IOperationResult
{
    public readonly string IndexName;

    public IndexOptimizeResult(string indexName)
    {
        IndexName = indexName;
    }

    public string Message { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(IndexName)] = IndexName,
            [nameof(Message)] = Message
        };
    }

    public bool ShouldPersist { get; } = false;

    bool IOperationResult.CanMerge => false;

    void IOperationResult.MergeWith(IOperationResult result)
    {
        throw new NotSupportedException();
    }
}
