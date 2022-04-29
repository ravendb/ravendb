using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForPatch : AbstractDocumentHandlerProcessorForPatch<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForPatch([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDocumentPatchAsync(string id, string changeVector, BlittableJsonReaderObject patchRequest, bool skipPatchIfChangeVectorMismatch, bool returnDebugInformation, bool test, TransactionOperationContext context)
    {
        var command = new PatchOperation.PatchCommand(id, changeVector, patchRequest, skipPatchIfChangeVectorMismatch, returnDebugInformation, test);

        int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, id);

        using (var token = RequestHandler.CreateOperationToken())
        {
            var proxyCommand = new ProxyCommand<PatchResult>(command, HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
