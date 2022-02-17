using System;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public interface IShardedOperation<T>
    {
        T Combine(Memory<T> results);

        RavenCommand<T> CreateCommandForShard(int shard);

        // if the return result is of type blittalbe
        JsonOperationContext CreateOperationContext() => throw new NotImplementedException($"Must be implemented for {typeof(T)}");
    }
}
