using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForHead<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForHead([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleHeadRequestAsync(string docId, string changeVector);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
        var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        await HandleHeadRequestAsync(id, changeVector);
    }
}
