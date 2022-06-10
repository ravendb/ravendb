using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractEtlHandlerProcessorForDebugStats<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<EtlTaskDebugStats[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractEtlHandlerProcessorForDebugStats([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<EtlTaskDebugStats[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetEtlTaskDebugStatsCommand(names, nodeTag);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
}
