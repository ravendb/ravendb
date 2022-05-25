using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public class ShardedBackupNowOperation : ShardedBackupOperationBase, IShardedOperation
    {
        public ShardedBackupNowOperation(ShardedDatabaseRequestHandler handler) : base(handler)
        {
        }

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            AddOperationFor(shardNumber);
            return new ShardedBackupNowCommand(QueryString);
        }
    }

    internal class ShardedBackupNowCommand : RavenCommand
    {
        private readonly string _queryString;

        public override bool IsReadRequest => false;

        public ShardedBackupNowCommand(string queryString)
        {
            _queryString = queryString;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{_queryString}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
            };

            return request;
        }
    }

}
