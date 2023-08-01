using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal sealed class ShardedDocumentHandlerProcessorForDelete : AbstractDocumentHandlerProcessorForDelete<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForDelete([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDeleteDocumentAsync(string docId, string changeVector)
    {
        var command = new DeleteDocumentCommand(docId, changeVector);

        int shardNumber;
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, docId);

        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var proxyCommand = new ProxyCommand(command, HttpContext.Response);

            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
