using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleDeleteDocumentAsync(string docId, string changeVector);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
        var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfMatch);

        await HandleDeleteDocumentAsync(id, changeVector);
    }
}
