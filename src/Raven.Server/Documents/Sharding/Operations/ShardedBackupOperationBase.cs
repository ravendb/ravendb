using System.Diagnostics;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Sharding.Handlers;

namespace Raven.Server.Documents.Sharding.Operations
{
    public interface IShardedBackupOperation : IShardedOperation
    {
        public long OperationId { get; }

        Task WaitForBackupToCompleteOnAllShards();
    }

    public class ShardedBackupOperationBase
    {
        public long OperationId { get; }

        protected readonly string QueryString;

        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly Task<IOperationResult>[] _operations;
        private readonly DocumentConventions _conventions = new()
        {
            OperationStatusFetchMode = OperationStatusFetchMode.Polling
        };

        protected ShardedBackupOperationBase(ShardedDatabaseRequestHandler handler)
        {
            _handler = handler;
            _operations = new Task<IOperationResult>[_handler.DatabaseContext.DatabaseRecord.Shards.Length];

            OperationId = _handler.DatabaseContext.Operations.GetNextOperationId();

            Debug.Assert(handler.HttpContext.Request.QueryString.Value != null);
            var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
            queryString["operationId"] = OperationId.ToString();

            QueryString = $"{handler.BaseShardUrl}?{queryString}";
        }


        protected void AddOperationFor(int shardNumber)
        {
            var op = new Operation(_handler.DatabaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber),
                changes: null,
                _conventions,
                OperationId);

            _operations[shardNumber] = op.WaitForCompletionAsync();
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public Task WaitForBackupToCompleteOnAllShards()
        {
            return Task.WhenAll(_operations);
        }
    }

}
