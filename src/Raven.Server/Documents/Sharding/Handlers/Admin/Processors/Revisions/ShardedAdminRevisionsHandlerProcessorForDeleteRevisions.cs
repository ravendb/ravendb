using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using static Raven.Server.Documents.Sharding.ShardLocator;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal sealed class ShardedAdminRevisionsHandlerProcessorForDeleteRevisions :  AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task<long> DeleteRevisionsAsync(DeleteRevisionsOperation.Parameters parameters, OperationCancelToken token)
        {
            if (parameters.DocumentIds.Count == 1)
            {
                var cmd = new DeleteRevisionsCommand(DocumentConventions.Default, parameters);
                int shardNumber;

                var config = RequestHandler.DatabaseContext.DatabaseRecord.Sharding;
                using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    shardNumber = ShardHelper.GetShardNumberFor(config, context, parameters.DocumentIds.Single());
                }

                var singleShardResult = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
                return singleShardResult.TotalDeletes;
            }

            Dictionary<int, IdsByShard<string>> shardsToDocs;
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                shardsToDocs = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, parameters.DocumentIds);

            var cmds = new Dictionary<int, DeleteRevisionsCommand>(shardsToDocs.Count);
            foreach (var (shard, ids) in shardsToDocs)
            {
                var shardParameters = parameters.Clone(ids.Ids.ToList());
                cmds[shard] = new DeleteRevisionsCommand(DocumentConventions.Default, shardParameters);
            }
            
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToDocs.Keys.ToArray(), new ShardedDeleteRevisionsOperation(RequestHandler.HttpContext, cmds), token.Token);
            return result.TotalDeletes;
        }


    }
}
