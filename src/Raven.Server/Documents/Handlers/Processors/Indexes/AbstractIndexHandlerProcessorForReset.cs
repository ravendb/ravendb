using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForReset<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForReset([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    private const string SideBySideQueryParameterName = "isSideBySide";

    protected override RavenCommand CreateCommandForNode(string nodeTag) => new ResetIndexOperation.ResetIndexCommand(GetName(), IsSideBySide(), nodeTag);

    protected string GetName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }

    protected bool IsSideBySide()
    {
        var sideBySideQueryParam = RequestHandler.GetBoolValueQueryString(SideBySideQueryParameterName, false);
        
        var sideBySide = false;
        
        if (sideBySideQueryParam.HasValue)
            sideBySide = sideBySideQueryParam.Value;

        return sideBySide;
    }
}
