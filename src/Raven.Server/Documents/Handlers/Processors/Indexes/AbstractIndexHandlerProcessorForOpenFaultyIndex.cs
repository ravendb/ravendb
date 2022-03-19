using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForOpenFaultyIndex<TRequestHandler, TOperationContext> : AbstractHandlerProxyActionProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForOpenFaultyIndex([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) 
        : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand CreateCommandForNode(string nodeTag) => new OpenFaultyIndexCommand(GetName(), nodeTag);

    protected string GetName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }
}
