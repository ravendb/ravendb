using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGet<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var ids = RequestHandler.GetStringValuesQueryString("id", required: false);
        var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            await GetDocumentsAsync(context, ids, metadataOnly);
        }
    }

    protected abstract ValueTask GetDocumentsAsync(TOperationContext context, StringValues ids, bool metadataOnly);
}
