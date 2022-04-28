using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Rachis
{
    internal abstract class AbstractRachisHandlerProcessorForWaitForIndexNotifications<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRachisHandlerProcessorForWaitForIndexNotifications([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask WaitForCommandsAsync(TransactionOperationContext context, WaitForIndexNotificationRequest commands);

        public override async ValueTask ExecuteAsync()
        {
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "raft-index-ids");
                var commands = JsonDeserializationServer.WaitForIndexNotificationRequest(blittable);

                await WaitForCommandsAsync(context, commands);
            }
        }
    }
}
