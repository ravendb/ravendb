using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration<TOperationContext> : AbstractAdminConfigurationHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext 
{
    protected AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration(AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            var clientConfigurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), Constants.Configuration.ClientId);
            var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

            await UpdateDatabaseRecordAsync(context, (record, index) =>
            {
                record.Client = clientConfiguration;
                record.Client.Etag = index;
            }, RequestHandler.GetRaftRequestIdFromQuery(), RequestHandler.DatabaseName);
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
        HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
    }
}
