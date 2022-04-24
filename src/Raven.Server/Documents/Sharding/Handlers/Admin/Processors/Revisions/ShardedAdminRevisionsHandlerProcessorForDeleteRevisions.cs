using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForDeleteRevisions :  AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask DeleteRevisionsAsync(TransactionOperationContext context, string[] documentIds)
        {
            var shardsToDocs = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, documentIds);
            var cmds = new Dictionary<int, RavenCommand>(shardsToDocs.Count);
            foreach (var (shard, ids) in shardsToDocs)
            {
                cmds[shard] = new DeleteRevisionsOperation.DeleteRevisionsCommand(DocumentConventions.Default, context,
                    new DeleteRevisionsOperation.Parameters() {DocumentIds = ids.Ids.ToArray()});
            }
            using(var token = RequestHandler.CreateOperationToken())
                await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToDocs.Keys.ToArray(), new ShardedDeleteRevisionsOperation(RequestHandler.HttpContext, cmds), token.Token);
        }
        
        internal readonly struct ShardedDeleteRevisionsOperation : IShardedOperation
        {
            private readonly Dictionary<int, RavenCommand> _cmds;
            private readonly HttpContext _httpContext;

            public ShardedDeleteRevisionsOperation(HttpContext httpContext, Dictionary<int, RavenCommand> cmds)
            {
                _cmds = cmds;
                _httpContext = httpContext;
            }
            
            public HttpRequest HttpRequest => _httpContext.Request;

            public RavenCommand<object> CreateCommandForShard(int shard) => _cmds[shard];

        }
    }
}
