using System;
using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct WaitForDatabaseContextUpdateOperation : IShardedOperation
    {
        private readonly List<long> _indexes;
        private readonly bool _useShardedName;

        public WaitForDatabaseContextUpdateOperation(List<long> indexes, bool useShardedName)
        {
            _indexes = indexes;
            _useShardedName = useShardedName;
        }

        public WaitForDatabaseContextUpdateOperation(long index, bool useShardedName) : this(new List<long> { index }, useShardedName)
        {
        }

        public object Combine(Memory<object> results) => throw new NotImplementedException();

        public RavenCommand<object> CreateCommandForShard(int shard)
            => new WaitForRaftCommands(_indexes, _useShardedName ? shard : null);
    }
}
