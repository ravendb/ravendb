using System;
using Raven.Client.Http;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public interface IShardedOperation<T>
    {
        T Combine(Memory<T> results);

        RavenCommand<T> CreateCommandForShard(int shard);
    }
}
