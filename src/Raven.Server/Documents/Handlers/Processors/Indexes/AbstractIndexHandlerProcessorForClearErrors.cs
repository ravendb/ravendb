using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForClearErrors<TRequestHandler, TOperationContext> : AbstractHandlerProxyActionProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForClearErrors([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) 
        : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand CreateCommandForNode(string nodeTag) => new DeleteIndexErrorsOperation.DeleteIndexErrorsCommand(GetNames(), nodeTag);

    protected StringValues GetNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
