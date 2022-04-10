using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForStale<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetIndexStalenessCommand.IndexStaleness, TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForStale([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected string GetName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

    protected override RavenCommand<GetIndexStalenessCommand.IndexStaleness> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();

        return new GetIndexStalenessCommand(name, nodeTag);
    }
}
