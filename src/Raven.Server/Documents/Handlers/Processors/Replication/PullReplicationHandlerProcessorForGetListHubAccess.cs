using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForGetListHubAccess : AbstractPullReplicationHandlerProcessorForGetListHubAccess<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public PullReplicationHandlerProcessorForGetListHubAccess([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;
    }
}
