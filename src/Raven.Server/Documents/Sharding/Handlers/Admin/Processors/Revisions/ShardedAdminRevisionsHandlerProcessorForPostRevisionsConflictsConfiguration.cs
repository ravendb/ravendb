using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration : AbstractAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }
    }
}
