using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal sealed class ShardedIndexHandlerProcessorForTestIndex : AbstractAdminIndexHandlerProcessorForTestIndex<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForTestIndex([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;
    
    protected override ValueTask HandleCurrentNodeAsync()
    {
        throw new NotSupportedException();
    }
    
    protected override RavenCommand<BlittableJsonReaderObject> CreateCommandForNode(string nodeTag)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var parameters = GetTestIndexParametersAsync(context).Result;

            var documentConventions = RequestHandler.ShardExecutor.Conventions;
            
            return new TestIndexCommand(documentConventions, nodeTag, parameters);
        }
    }
    
    protected override async Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
