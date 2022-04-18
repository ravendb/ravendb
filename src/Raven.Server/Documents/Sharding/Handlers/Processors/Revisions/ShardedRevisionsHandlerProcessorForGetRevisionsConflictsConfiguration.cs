using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration : AbstractRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override RevisionsCollectionConfiguration GetRevisionsConflicts(TransactionOperationContext context)
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.RevisionsForConflicts;
        }
    }
}
