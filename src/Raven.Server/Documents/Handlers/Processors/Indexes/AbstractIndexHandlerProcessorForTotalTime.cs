using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForTotalTime<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetIndexesTotalTimeCommand.IndexTotalTime[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForTotalTime([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);

    protected override RavenCommand<GetIndexesTotalTimeCommand.IndexTotalTime[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetIndexesTotalTimeCommand(names, nodeTag);
    }
}
