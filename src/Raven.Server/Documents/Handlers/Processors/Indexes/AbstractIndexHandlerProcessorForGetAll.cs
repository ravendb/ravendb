using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
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

    protected override async ValueTask WriteResultAsync(IndexDefinition[] result)
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
