using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForPerformance<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexPerformanceStats[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForPerformance([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);

    protected override RavenCommand<IndexPerformanceStats[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetIndexPerformanceStatisticsOperation.GetIndexPerformanceStatisticsCommand(names, nodeTag);
    }
}
