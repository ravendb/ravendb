using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration
{
    internal abstract class AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration(TRequestHandler requestHandler)
            : base(requestHandler)
        {

        }

        protected abstract ValueTask WaitForIndexNotificationAsync(long index);

        public override async ValueTask ExecuteAsync()
        {
            using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var studioConfigurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), Constants.Configuration.StudioId);
                var studioConfiguration = JsonDeserializationServer.StudioConfiguration(studioConfigurationJson);

                var command = new PutDatabaseStudioConfigurationCommand(studioConfiguration, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());

                long index = (await RequestHandler.Server.ServerStore.SendToLeaderAsync(command)).Index;

                await WaitForIndexNotificationAsync(index);
            }

            RequestHandler.NoContentStatus(HttpStatusCode.Created);
        }
    }
}
