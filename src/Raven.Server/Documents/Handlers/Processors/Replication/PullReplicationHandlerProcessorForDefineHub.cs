using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForDefineHub : AbstractPullReplicationHandlerProcessorForDefineHub<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public PullReplicationHandlerProcessorForDefineHub([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
