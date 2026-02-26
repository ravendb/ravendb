using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForGetRemoteAttachmentsConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStudioDatabaseTasksHandlerProcessorForGetRemoteAttachmentsConfiguration([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract ValueTask<RemoteAttachmentsConfiguration> GetRemoteAttachmentsConfigurationAsync(TransactionOperationContext context);

    public override async ValueTask ExecuteAsync()
    {
        RemoteAttachmentsConfiguration configuration;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            configuration = await GetRemoteAttachmentsConfigurationAsync(context);
        }

        if (configuration == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext jsonContext))
        await using (var writer = new AsyncBlittableJsonTextWriter(jsonContext, RequestHandler.ResponseBodyStream()))
            jsonContext.Write(writer, configuration.ToStudioJson());
    }
}
