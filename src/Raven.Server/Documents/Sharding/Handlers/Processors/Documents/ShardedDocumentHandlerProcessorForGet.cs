using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForGet : AbstractDocumentHandlerProcessorForGet<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForGet([NotNull] ShardedDocumentHandler requestHandler, [NotNull] JsonContextPoolBase<TransactionOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override async ValueTask GetDocumentsAsync(TransactionOperationContext context, StringValues ids, bool metadataOnly)
    {
        var includePaths = RequestHandler.GetStringValuesQueryString("include", required: false);
        var etag = RequestHandler.GetStringFromHeaders("If-None-Match");

        if (ids.Count > 0)
        {
            await RequestHandler.GetDocumentsByIdAsync(ids, includePaths, etag, metadataOnly, context);
        }
        else
        {
            await RequestHandler.GetDocumentsAsync(context, metadataOnly);
        }
    }
}
