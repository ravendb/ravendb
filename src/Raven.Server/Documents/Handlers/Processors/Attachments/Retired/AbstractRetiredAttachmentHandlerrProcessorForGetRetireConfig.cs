using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired;

internal abstract class AbstractRetiredAttachmentHandlerrProcessorForGetRetireConfig<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractRetiredAttachmentHandlerrProcessorForGetRetireConfig([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask<RetireAttachmentsConfiguration> GetAttachmentRetireConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var expirationConfig = await GetAttachmentRetireConfiguration();

        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            if (expirationConfig != null)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, expirationConfig.ToJson());
                }
            }
            else
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            }
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
            writer.WritePropertyName(nameof(GetAttachmentHashCountCommand.Response.RegularCount));
            writer.WriteInteger(response.RegularCount);
            writer.WriteComma();
            writer.WritePropertyName(nameof(GetAttachmentHashCountCommand.Response.RetiredCount));
            writer.WriteInteger(response.RetiredCount);
            writer.WriteComma();
            writer.WritePropertyName(nameof(GetAttachmentHashCountCommand.Response.TotalCount));
            writer.WriteInteger(response.TotalCount);
            writer.WriteEndObject();
        }
    }
}
