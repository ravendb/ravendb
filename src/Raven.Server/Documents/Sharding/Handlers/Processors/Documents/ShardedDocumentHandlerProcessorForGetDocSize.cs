using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForGetDocSize : AbstractDocumentHandlerProcessorForGetDocSize<BlittableJsonReaderObject, ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForGetDocSize([NotNull] ShardedDocumentHandler requestHandler, [NotNull] JsonContextPoolBase<TransactionOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override void WriteDocSize(BlittableJsonReaderObject size, TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        context.Write(writer, size);
    }

    protected override async ValueTask<(HttpStatusCode StatusCode, BlittableJsonReaderObject SizeResult)> GetResultAndStatusCodeAsync(string docId, TransactionOperationContext context)
    {
        var index = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

        var cmd = new ShardedCommand(RequestHandler, Headers.None);
        await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);

        return (cmd.StatusCode, cmd.Result);
    }
}
