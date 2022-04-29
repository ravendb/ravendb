using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetRevisionsConfiguration : AbstractRevisionsHandlerProcessorForGetRevisionsConfiguration<ShardedDatabaseRequestHandler>
    {
        public ShardedRevisionsHandlerProcessorForGetRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override RevisionsConfiguration GetRevisionsConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.Revisions;
        }
    }
}
