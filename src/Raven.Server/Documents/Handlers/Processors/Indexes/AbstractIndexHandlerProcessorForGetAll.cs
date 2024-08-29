using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Microsoft.IdentityModel.Tokens;
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

    protected StringValues GetNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }

    protected abstract IndexDefinition[] GetIndexDefinitions(StringValues indexNames, int start, int pageSize);

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var names = GetNames();
        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var indexDefinitions = GetIndexDefinitions(names, start, pageSize);

        return WriteResultAsync(indexDefinitions);
    }

    protected override RavenCommand<IndexDefinition[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();
        if (!names.IsNullOrEmpty())
            return new GetIndexesOperation.GetIndexesCommand(names, nodeTag);

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
