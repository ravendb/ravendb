using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForHead<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForHead([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleHeadRequest(string docId, string changeVector);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
        var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        await HandleHeadRequest(id, changeVector);
    }
}
