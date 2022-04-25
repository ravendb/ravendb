using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGetDocSize<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForGetDocSize([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleDocSize(string docId);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

        await HandleDocSize(id);
    }
}
