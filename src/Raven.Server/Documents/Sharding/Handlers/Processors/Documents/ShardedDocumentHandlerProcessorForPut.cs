using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal sealed class ShardedDocumentHandlerProcessorForPut : AbstractDocumentHandlerProcessorForPut<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForPut([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDocumentPutAsync(string id, string changeVector, BlittableJsonReaderObject doc, TransactionOperationContext context)
    {
        var command = new PutDocumentCommand(RequestHandler.ShardExecutor.Conventions, id, changeVector, doc);

        int shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, id);

        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var proxyCommand = new ProxyCommand<PutResult>(command, HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
