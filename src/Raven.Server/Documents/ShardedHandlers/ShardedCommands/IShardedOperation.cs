using System;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public interface IShardedOperation<TResult> : IShardedOperation<TResult, TResult>
    {
    }

    public interface IShardedOperation<TResult, out TCombinedResult>
    {
        TCombinedResult Combine(Memory<TResult> results);

        RavenCommand<TResult> CreateCommandForShard(int shard);

        // if the return result is of type blittalbe
        JsonOperationContext CreateOperationContext() => throw new NotImplementedException($"Must be implemented for {typeof(TCombinedResult)}");
    }
}
