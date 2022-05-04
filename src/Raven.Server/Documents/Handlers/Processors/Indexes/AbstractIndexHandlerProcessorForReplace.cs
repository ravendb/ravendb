using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForReplace<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForReplace([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected string GetIndexName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var indexName = GetIndexName();
        return new ReplaceIndexCommand(indexName, nodeTag);
    }
}
