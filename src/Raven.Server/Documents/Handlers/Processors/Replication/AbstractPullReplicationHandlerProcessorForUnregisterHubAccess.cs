using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForUnregisterHubAccess<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractPullReplicationHandlerProcessorForUnregisterHubAccess([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var hub = RequestHandler.GetStringQueryString("name", true);
            var thumbprint = RequestHandler.GetStringQueryString("thumbprint", true);

            var command = new UnregisterReplicationHubAccessCommand(RequestHandler.DatabaseName, hub, thumbprint, raftRequestId);
            return await RequestHandler.Server.ServerStore.SendToLeaderAsync(command);
        }
    }
}
