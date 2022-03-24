using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Streaming;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public interface IShardedOperation<TResult> : IShardedOperation<TResult, TResult>
    {
    }

    public interface IShardedOperation : IShardedOperation<object>
    {
        object IShardedOperation<object, object>.Combine(Memory<object> results) => null;
    }

    public interface IShardedOperation<TResult, out TCombinedResult>
    {
        TCombinedResult Combine(Memory<TResult> results);

        TCombinedResult CombineCommands(Memory<RavenCommand<TResult>> commands, Memory<TResult> results)
        {
            var span = commands.Span;
            for (int i = 0; i < span.Length; i++)
            {
                results.Span[i] = span[i].Result;
            }

            return Combine(results);
        }

        RavenCommand<TResult> CreateCommandForShard(int shard);

        // if the return result is of type blittalbe
        JsonOperationContext CreateOperationContext() => throw new NotImplementedException($"Must be implemented for {typeof(TCombinedResult)}");
    }

    public interface IShardedStreamableOperation : IShardedOperation<StreamResult, CombinedStreamResult>
    {
        CombinedStreamResult IShardedOperation<StreamResult, CombinedStreamResult>.Combine(Memory<StreamResult> results) =>
            new CombinedStreamResult { Results = results };
    }
}
