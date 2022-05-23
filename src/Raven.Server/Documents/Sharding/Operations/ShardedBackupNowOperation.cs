using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct ShardedBackupNowOperation : IShardedOperation
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly long _operationId;
        private readonly Task<IOperationResult>[] _operations;
        private readonly string _queryString;
        private readonly DocumentConventions _conventions;

        public ShardedBackupNowOperation(ShardedDatabaseRequestHandler handler, long operationId)
        {
            _handler = handler;
            _operationId = operationId;
            _operations = new Task<IOperationResult>[_handler.DatabaseContext.DatabaseRecord.Shards.Length];
            _conventions = new DocumentConventions
            {
                OperationStatusFetchMode = OperationStatusFetchMode.Polling
            };

            Debug.Assert(handler.HttpContext.Request.QueryString.Value != null);
            var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
            queryString["operationId"] = operationId.ToString();

            _queryString = $"{handler.BaseShardUrl}?{queryString}";
        }

        public RavenCommand<object> CreateCommandForShard(int shard)
        {
            var op = new Operation(_handler.DatabaseContext.ShardExecutor.GetRequestExecutorAt(shard),
                changes: null,
                _conventions,
                _operationId);

            _operations[shard] = op.WaitForCompletionAsync();

            return new ShardedBackupNowCommand(_queryString);
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public Task WaitForBackupToCompleteOnAllShards()
        {
            return Task.WhenAll(_operations);
        }

        private class ShardedBackupNowCommand : RavenCommand
        {
            private readonly string _queryString;

            public override bool IsReadRequest => true;

            public ShardedBackupNowCommand(string queryString)
            {
                _queryString = queryString;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}{_queryString}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
