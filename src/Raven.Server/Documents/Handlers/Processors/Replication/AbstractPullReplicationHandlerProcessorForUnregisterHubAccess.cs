using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForUnregisterHubAccess<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractPullReplicationHandlerProcessorForUnregisterHubAccess([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask AssertCanExecuteAsync(string databaseName)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            return base.AssertCanExecuteAsync(databaseName);
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var hub = RequestHandler.GetStringQueryString("name", true);
            var thumbprint = RequestHandler.GetStringQueryString("thumbprint", true);

            var command = new UnregisterReplicationHubAccessCommand(databaseName, hub, thumbprint, raftRequestId);
            return await RequestHandler.Server.ServerStore.SendToLeaderAsync(command);
        }
    }
}
