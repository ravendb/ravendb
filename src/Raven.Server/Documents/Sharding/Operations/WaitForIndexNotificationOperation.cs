using System;
using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct WaitForIndexNotificationOperation : IShardedOperation
    {
        private readonly List<long> _indexes;

        public WaitForIndexNotificationOperation(List<long> indexes)
        {
            _indexes = indexes;
        }

        public WaitForIndexNotificationOperation(long index) : this(new List<long>(capacity: 1) { index })
        {
        }

        public object Combine(Memory<object> results) => throw new NotImplementedException();

        public RavenCommand<object> CreateCommandForShard(int shard)
            =>  new Documents.Commands.WaitForIndexNotificationOperation.WaitForIndexNotificationCommand(_indexes);
    }
}
