using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
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
    
    private const string IndexResetModeQueryStringParamName = "mode";
    
    protected override RavenCommand CreateCommandForNode(string nodeTag) => new ResetIndexOperation.ResetIndexCommand(GetName(), GetIndexResetMode(), nodeTag);

    protected string GetName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }

    private IndexResetMode? GetIndexResetMode()
    {
        var indexResetModeQueryParam = RequestHandler.GetStringQueryString(IndexResetModeQueryStringParamName, false);

        if (indexResetModeQueryParam is null)
            return null;
            
        return Enum.Parse<IndexResetMode>(indexResetModeQueryParam);
    }
}
