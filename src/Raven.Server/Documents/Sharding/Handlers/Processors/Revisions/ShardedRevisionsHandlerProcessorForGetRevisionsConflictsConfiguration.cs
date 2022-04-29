using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration : AbstractRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration<ShardedDatabaseRequestHandler>
    {
        public ShardedRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override RevisionsCollectionConfiguration GetRevisionsConflicts()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.RevisionsForConflicts;
        }
    }
}
