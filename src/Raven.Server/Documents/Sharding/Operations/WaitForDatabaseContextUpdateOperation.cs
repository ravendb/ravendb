using System;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct WaitForDatabaseContextUpdateOperation : IShardedOperation
    {
        private readonly string _database;
        private readonly long _index;

        public WaitForDatabaseContextUpdateOperation(string database, long index)
        {
            _database = database;
            _index = index;
        }
        public object Combine(Memory<object> results) => throw new NotImplementedException();

        public RavenCommand<object> CreateCommandForShard(int shard)
            => new WaitForDatabaseContextUpdateCommand(_database, _index);

        private class WaitForDatabaseContextUpdateCommand : RavenCommand
        {
            private readonly string _database;
            private readonly long _index;

            public WaitForDatabaseContextUpdateCommand(string database, long index)
            {
                _database = database;
                _index = index;
            }
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/sharded/wait-for?database={Uri.EscapeDataString(_database)}&index={_index}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
                return request;
            }
        }
    }
}
