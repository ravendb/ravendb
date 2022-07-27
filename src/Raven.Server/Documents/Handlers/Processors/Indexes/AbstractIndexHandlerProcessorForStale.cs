using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForStale<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetIndexStalenessCommand.IndexStaleness, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForStale([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected string GetName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

    protected override RavenCommand<GetIndexStalenessCommand.IndexStaleness> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();

        return new GetIndexStalenessCommand(name, nodeTag);
    }
}
