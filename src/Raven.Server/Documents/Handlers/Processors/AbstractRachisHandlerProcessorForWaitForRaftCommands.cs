using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractRachisHandlerProcessorForWaitForRaftCommands<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractRachisHandlerProcessorForWaitForRaftCommands([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask WaitForCommandsAsync(WaitForCommandsRequest commands);

        public override async ValueTask ExecuteAsync()
        {
            WaitForCommandsRequest commands;
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "raft-index-ids");
                commands = JsonDeserializationServer.WaitForRaftCommands(blittable);
            }

            await WaitForCommandsAsync(commands);
        }
    }
}
