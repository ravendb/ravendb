using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;


namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal sealed class ShardedRevisionsBinCleanerHandlerProcessorForGetConfiguration : AbstractDatabaseHandlerProcessorForGetConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext, RevisionsBinConfiguration>
    {
        public ShardedRevisionsBinCleanerHandlerProcessorForGetConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RevisionsBinConfiguration GetConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.RevisionsBin;
        }
    }
}
