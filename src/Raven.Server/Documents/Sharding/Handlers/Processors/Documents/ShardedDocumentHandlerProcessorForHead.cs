using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForHead : AbstractDocumentHandlerProcessorForHead<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForHead([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleHeadRequest(string docId, string changeVector)
    {
        var command = new HeadDocumentCommand(docId, changeVector);

        int shardNumber;
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

        using (var token = RequestHandler.CreateOperationToken())
        {
            var proxyCommand = new ProxyCommand<string>(command, HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
