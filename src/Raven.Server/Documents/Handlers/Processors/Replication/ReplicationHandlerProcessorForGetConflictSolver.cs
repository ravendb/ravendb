using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication;

internal sealed class ReplicationHandlerProcessorForGetConflictSolver : AbstractReplicationHandlerProcessorForGetConflictSolver<DatabaseRequestHandler, DocumentsOperationContext>
{
    public ReplicationHandlerProcessorForGetConflictSolver([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
