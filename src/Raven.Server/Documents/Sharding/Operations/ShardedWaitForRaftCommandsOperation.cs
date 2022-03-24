using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct ShardedWaitForRaftCommandsOperation : IShardedOperation
    {
        private readonly List<long> _raftCommandIndexes;

        public ShardedWaitForRaftCommandsOperation(List<long> raftCommandIndexes)
        {
            _raftCommandIndexes = raftCommandIndexes;
        }

        RavenCommand<object> IShardedOperation<object, object>.CreateCommandForShard(int shard)
            => new WaitForRaftCommands(_raftCommandIndexes);
    }
}
