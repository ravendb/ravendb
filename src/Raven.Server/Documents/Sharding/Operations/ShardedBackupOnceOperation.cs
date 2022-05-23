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
    public readonly struct ShardedBackupOnceOperation : IShardedOperation
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly BlittableJsonReaderObject _config;
        private readonly long _operationId;
        private readonly string _queryString;
        private readonly Task<IOperationResult>[] _operations;
        private readonly DocumentConventions _conventions;

        public ShardedBackupOnceOperation(ShardedDatabaseRequestHandler handler, long operationId, BlittableJsonReaderObject config)
        {
            _handler = handler;
            _config = config;
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

            return new ShardedBackupOnceCommand(_config, _queryString);
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public Task WaitForBackupToCompleteOnAllShards()
        {
            return Task.WhenAll(_operations);
        }

        private class ShardedBackupOnceCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _content;
            private readonly string _queryString;

            public override bool IsReadRequest => false;

            public ShardedBackupOnceCommand(BlittableJsonReaderObject backupConfig, string queryString)
            {
                _content = backupConfig;
                _queryString = queryString;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}{_queryString}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async (stream) => await _content.WriteJsonToAsync(stream)),
                };

                return request;
            }
        }
    }
}
