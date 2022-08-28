using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForUnregisterHubAccess : AbstractPullReplicationHandlerProcessorForUnregisterHubAccess<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public PullReplicationHandlerProcessorForUnregisterHubAccess([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask AssertCanExecuteAsync()
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            return base.AssertCanExecuteAsync();
        }
    }
}
