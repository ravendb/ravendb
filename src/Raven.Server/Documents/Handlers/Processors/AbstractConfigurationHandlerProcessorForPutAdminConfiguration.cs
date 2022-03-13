using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractConfigurationHandlerProcessorForPutAdminConfiguration<TRequestHandler, TOperationContext> : AbstractConfigurationHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractConfigurationHandlerProcessorForPutAdminConfiguration(TRequestHandler requestHandler, JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        { }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var studioConfigurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), Constants.Configuration.StudioId);
                var studioConfiguration = JsonDeserializationServer.StudioConfiguration(studioConfigurationJson);

                await UpdateDatabaseRecordAsync(context, (record, _) =>
                {
                    record.Studio = studioConfiguration;
                }, RequestHandler.GetRaftRequestIdFromQuery(), GetDatabaseName());
            }

            RequestHandler.NoContentStatus(HttpStatusCode.Created);
        }
    }
}
