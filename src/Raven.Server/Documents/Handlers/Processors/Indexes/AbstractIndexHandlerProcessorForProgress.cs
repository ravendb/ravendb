using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForProgress<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexProgress[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<IndexProgress[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();
        var exact = IsExact();
        return new GetIndexesProgressCommand(nodeTag, names, exact);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);

    protected bool IsExact() => RequestHandler.GetBoolValueQueryString("exact", required: false) ?? false;
}
