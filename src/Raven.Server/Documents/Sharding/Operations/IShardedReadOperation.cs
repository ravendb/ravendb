using System;
using System.Collections.Generic;
using System.Net;
using Raven.Server.Documents.Sharding.Executors;

namespace Raven.Server.Documents.Sharding.Operations;

public interface IShardedReadOperation<TResult> : IShardedReadOperation<TResult, TResult>
{

}

public interface IShardedReadOperation<TResult, TCombinedResult> : IShardedOperation<TResult, ShardedReadResult<TCombinedResult>>
{
    string ExpectedEtag { get; }

    string CombineCommandsEtag(Dictionary<int, ShardExecutionResult<TResult>> results) => ComputeHttpEtags.CombineEtags(results);

    ShardedReadResult<TCombinedResult> IShardedOperation<TResult, ShardedReadResult<TCombinedResult>>.Combine(Dictionary<int, ShardExecutionResult<TResult>> results) => throw new NotSupportedException();
    TCombinedResult CombineResults(Dictionary<int, ShardExecutionResult<TResult>> results);

    ShardedReadResult<TCombinedResult> IShardedOperation<TResult, ShardedReadResult<TCombinedResult>>.CombineCommands(Dictionary<int, ShardExecutionResult<TResult>> results)
    {
        var actualEtag = CombineCommandsEtag(results);

        var result = new ShardedReadResult<TCombinedResult>
        {
            CombinedEtag = actualEtag
        };

        if (ExpectedEtag == result.CombinedEtag)
        {
            result.StatusCode = (int)HttpStatusCode.NotModified;
            return result;
        }

        result.Result = CombineResults(results);
        return result;
    }
}
public sealed class ShardedReadResult<T>
{
    public T Result;
    public int StatusCode;
    public string CombinedEtag;
}
