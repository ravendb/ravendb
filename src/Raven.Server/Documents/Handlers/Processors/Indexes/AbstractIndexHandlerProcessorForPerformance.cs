using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForPerformance<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexPerformanceStats[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForPerformance([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);

    protected override RavenCommand<IndexPerformanceStats[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetIndexPerformanceStatisticsOperation.GetIndexPerformanceStatisticsCommand(names, nodeTag);
    }
}
