using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetAll<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexDefinition[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForGetAll([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name", required: false);
    }

    protected abstract IndexDefinition[] GetIndexDefinitions(string indexName, int start, int pageSize);

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var indexDefinitions = GetIndexDefinitions(name, start, pageSize);

        return WriteResultAsync(indexDefinitions);
    }

    protected override RavenCommand<IndexDefinition[]> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();
        if (name != null)
            return new GetIndexesOperation.GetIndexesCommand(name, nodeTag);

        return new GetIndexesOperation.GetIndexesCommand(RequestHandler.GetStart(), RequestHandler.GetPageSize(), nodeTag);
    }

    private async ValueTask WriteResultAsync(IndexDefinition[] result)
    {
        if (result == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", result, (w, c, indexDefinition) =>
            {
                w.WriteIndexDefinition(c, indexDefinition);
            });

            writer.WriteEndObject();
        }
    }
}
