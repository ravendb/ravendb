using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Commands.Queries;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForPatchTest : AbstractQueriesHandlerProcessorForPatchTest<ShardedQueriesHandler, TransactionOperationContext>
{
    public ShardedQueriesHandlerProcessorForPatchTest([NotNull] ShardedQueriesHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache)
    {
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.DatabaseContext.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.DatabaseContext.Configuration;

    protected override async ValueTask HandleDocumentPatchTestAsync(IndexQueryServerSide query, string docId, TransactionOperationContext context)
    {
        var command = new PatchByQueryTestCommand(docId, query);

        int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

        using (var token = RequestHandler.CreateOperationToken())
        {
            var proxyCommand = new ProxyCommand<PatchByQueryTestCommand.Response>(command, HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
