using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetAllNames<TRequestHandler, TOperationContext> : AbstractHandlerReadProcessor<string[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForGetAllNames([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name", required: false);
    }

    protected override RavenCommand<string[]> CreateCommandForNode(string nodeTag) => new GetIndexNamesOperation.GetIndexNamesCommand(RequestHandler.GetStart(), RequestHandler.GetPageSize());

    protected override async ValueTask WriteResultAsync(string[] result)
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

            writer.WriteArray(context, "Results", result, (w, c, name) =>
            {
                w.WriteString(name);
            });

            writer.WriteEndObject();
        }
    }
}
