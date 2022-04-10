using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetAll<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexDefinition[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForGetAll([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name", required: false);
    }

    protected override RavenCommand<IndexDefinition[]> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();
        if (name != null)
            return new GetIndexesOperation.GetIndexesCommand(name, nodeTag);
        
        return new GetIndexesOperation.GetIndexesCommand(RequestHandler.GetStart(), RequestHandler.GetPageSize(), nodeTag);
    }
}
