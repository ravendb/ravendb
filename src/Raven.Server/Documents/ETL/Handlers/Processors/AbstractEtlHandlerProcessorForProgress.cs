using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractEtlHandlerProcessorForProgress<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<EtlTaskProgress[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractEtlHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<EtlTaskProgress[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();
        var exact = RequestHandler.GetBoolValueQueryString("exact", required: false) ?? false;

        return new GetEtlTaskProgressCommand(names, nodeTag, exact);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
}
