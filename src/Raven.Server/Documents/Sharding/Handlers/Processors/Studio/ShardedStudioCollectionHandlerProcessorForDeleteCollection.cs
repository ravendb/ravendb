using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal class ShardedStudioCollectionHandlerProcessorForDeleteCollection : AbstractStudioCollectionHandlerProcessorForDeleteCollection<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioCollectionHandlerProcessorForDeleteCollection([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteCollectionAsync(TransactionOperationContext context, IDisposable returnContextToPool, string collectionName, HashSet<string> excludeIds, long operationId)
        {
            var shardToIds = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, excludeIds);
            
            using (var token = RequestHandler.CreateOperationToken())
            {
                var op = new ShardedDeleteStudioCollectionOperation(HttpContext, operationId, collectionName, shardToIds);
                await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token.Token);
            }

            returnContextToPool.Dispose();
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }
    }

    internal readonly struct ShardedDeleteStudioCollectionOperation : IShardedOperation
    {
        private readonly HttpContext _httpContext;
        private readonly long _operationId;
        private readonly string _collectionName;
        private readonly Dictionary<int, ShardLocator.IdsByShard<string>> _shardToExcludeIds;

        public ShardedDeleteStudioCollectionOperation(HttpContext httpContext, long operationId, string collectionName, Dictionary<int, ShardLocator.IdsByShard<string>> shardToExcludeIds)
        {
            _httpContext = httpContext;
            _operationId = operationId;
            _collectionName = collectionName;
            _shardToExcludeIds = shardToExcludeIds;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public object Combine(Memory<object> results)
        {
            return null;
        }

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            List<string> excludeIds = null;

            if (_shardToExcludeIds.ContainsKey(shardNumber))
                excludeIds = _shardToExcludeIds[shardNumber].Ids;

            return new DeleteStudioCollectionOperation.DeleteStudioCollectionCommand(_operationId, _collectionName, excludeIds);
        }
    }
}
