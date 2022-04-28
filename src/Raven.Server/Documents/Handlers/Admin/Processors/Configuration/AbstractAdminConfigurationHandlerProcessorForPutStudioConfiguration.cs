using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration
{
    internal abstract class AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<TOperationContext> : AbstractAdminConfigurationHandlerProcessor<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration(AbstractDatabaseRequestHandler<TOperationContext> requestHandler) : base(requestHandler)
        { }

        public override async ValueTask ExecuteAsync()
        {
            using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var studioConfigurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), Constants.Configuration.StudioId);
                var studioConfiguration = JsonDeserializationServer.StudioConfiguration(studioConfigurationJson);

                await UpdateDatabaseRecordAsync(context, (record, _) =>
                {
                    record.Studio = studioConfiguration;
                }, RequestHandler.GetRaftRequestIdFromQuery(), RequestHandler.DatabaseName);
            }

            RequestHandler.NoContentStatus(HttpStatusCode.Created);
        }
    }
}
