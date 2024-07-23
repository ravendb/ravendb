using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForDeleteRevisions : AbstractRevisionsHandlerProcessorForDeleteRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForDeleteRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task DeleteRevisions(DeleteRevisionsIntrenalRequest request, OperationCancelToken token)
        {
            var requestPerShard = new Dictionary<int, DeleteRevisionsIntrenalRequest>();
            var config = RequestHandler.DatabaseContext.DatabaseRecord.Sharding;

            if (request.RevisionsChangeVecotors.IsNullOrEmpty() == false)
            {
                // doing that because we cannot use 'GetShardNumberFor' for cv, only for id
                var shards = config.Shards.Keys.ToList();
                foreach (var shardNumber in shards)
                {
                    requestPerShard[shardNumber] = new DeleteRevisionsIntrenalRequest()
                    {
                        MaxDeletes = request.MaxDeletes, 
                        RevisionsChangeVecotors = request.RevisionsChangeVecotors, 
                        ThrowIfChangeVectorsNotFound = false
                    };
                }
            }

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                foreach (var id in request.DocumentIds)
                {
                    var shardNumber = ShardHelper.GetShardNumberFor(config, context, id);

                    if (requestPerShard.TryGetValue(shardNumber, out var shardRequest) == false)
                    {
                        shardRequest = new DeleteRevisionsIntrenalRequest()
                        {
                            MaxDeletes = request.MaxDeletes,
                            DocumentIds = new List<string>()
                        };
                        requestPerShard.Add(shardNumber, shardRequest);
                    }
                    else if (shardRequest.DocumentIds == null)
                    {
                        shardRequest.DocumentIds = new List<string>();
                    }

                    shardRequest.DocumentIds.Add(id);
                }
            }

            var op = new ShardedDeleteRevisionsManuallyOperation(requestPerShard);
            return RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(requestPerShard.Keys.ToArray(), op, token.Token);
        }
    }
}
