using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Remote;

internal abstract class AbstractRemoteAttachmentHandlerProcessorForGetRemoteConfig<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractRemoteAttachmentHandlerProcessorForGetRemoteConfig([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask<RemoteAttachmentsConfiguration> GetAttachmentRemoteConfigurationAsync();

    public override async ValueTask ExecuteAsync()
    {
        var expirationConfig = await GetAttachmentRemoteConfigurationAsync();

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
}
