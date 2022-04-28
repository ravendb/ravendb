using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal abstract class AbstractAttachmentHandlerProcessorForGetHashCount<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAttachmentHandlerProcessorForGetHashCount([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask<GetAttachmentHashCountCommand.Response> GetResponseAsync(TOperationContext context, string hash);

    public override async ValueTask ExecuteAsync()
    {
        var hash = RequestHandler.GetStringQueryString("hash");

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var response = await GetResponseAsync(context, hash);

            await WriteResponseAsync(context, response);
        }
    }

    private async ValueTask WriteResponseAsync(TOperationContext context, GetAttachmentHashCountCommand.Response response)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(GetAttachmentHashCountCommand.Response.Hash));
            writer.WriteString(response.Hash);
            writer.WriteComma();
            writer.WritePropertyName(nameof(GetAttachmentHashCountCommand.Response.Count));
            writer.WriteInteger(response.Count);
            writer.WriteEndObject();
        }
    }
}
