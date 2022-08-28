using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetAllNames : AbstractIndexHandlerProcessorForGetAllNames<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAllNames([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();

        var indexDefinitions = IndexHandlerProcessorForGetAll.GetIndexDefinitions(RequestHandler, name);

        var names = indexDefinitions?
            .Select(x => x.Name)
            .ToArray();

        await WriteResultAsync(names);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<string[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

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
