using System;
using System.Net;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public interface IShardedReadOperation<TResult> : IShardedReadOperation<TResult, TResult>
{

}

public interface IShardedReadOperation<TResult, TCombinedResult> : IShardedOperation<TResult, ShardedReadResult<TCombinedResult>> 
{
    string ExpectedEtag { get; }

    string CombineCommandsEtag(Memory<RavenCommand<TResult>> commands) => ComputeHttpEtags.CombineEtags(commands);

    ShardedReadResult<TCombinedResult> IShardedOperation<TResult, ShardedReadResult<TCombinedResult>>.Combine(Memory<TResult> results) => throw new NotSupportedException();
    TCombinedResult CombineResults(Memory<TResult> results);

    ShardedReadResult<TCombinedResult> IShardedOperation<TResult, ShardedReadResult<TCombinedResult>>.CombineCommands(Memory<RavenCommand<TResult>> commands, Memory<TResult> results)
    {
        var actualEtag = CombineCommandsEtag(commands);

        var result = new ShardedReadResult<TCombinedResult>
        {
            CombinedEtag = actualEtag
        };

        var span = commands.Span;

        if (ExpectedEtag == result.CombinedEtag)
        {
            var allNotModified = true;
            foreach (var cmd in span)
            {
                if (cmd.StatusCode == HttpStatusCode.NotModified) 
                    continue;
                
                allNotModified = false;
                break;
            }

            if (allNotModified)
            {
                result.StatusCode = (int)HttpStatusCode.NotModified;
                return result;
            }
        }

        for (int i = 0; i < span.Length; i++)
        {
            results.Span[i] = span[i].Result;
        }

        result.Result = CombineResults(results);
        return result;
    }
}
public class ShardedReadResult<T>
{
    public T Result;
    public int StatusCode;
    public string CombinedEtag;
}
