using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetAllNames<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<string[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForGetAllNames([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name", required: false);
    }

    protected abstract string[] GetIndexNames(string name);

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var names = GetIndexNames(name)
            .OrderBy(x => x)
            .Skip(start)
            .Take(pageSize)
            .ToArray();

        await WriteResultAsync(names);
    }

    protected override RavenCommand<string[]> CreateCommandForNode(string nodeTag) => new GetIndexNamesOperation.GetIndexNamesCommand(RequestHandler.GetStart(), RequestHandler.GetPageSize(), nodeTag);

    private async ValueTask WriteResultAsync(string[] result)
    {
        if (result == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
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
