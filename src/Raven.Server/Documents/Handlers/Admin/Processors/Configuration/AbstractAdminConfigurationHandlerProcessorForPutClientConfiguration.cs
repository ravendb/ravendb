using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration<TRequestHandler, TOperationContext> : AbstractAdminConfigurationHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration(TRequestHandler requestHandler, JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var clientConfigurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), Constants.Configuration.ClientId);
            var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

            await UpdateDatabaseRecordAsync(context, (record, index) =>
            {
                record.Client = clientConfiguration;
                record.Client.Etag = index;
            }, RequestHandler.GetRaftRequestIdFromQuery(), GetDatabaseName());
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
        HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
    }
}
