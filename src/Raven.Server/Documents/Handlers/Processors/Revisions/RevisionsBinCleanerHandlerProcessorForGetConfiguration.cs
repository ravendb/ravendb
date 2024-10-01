using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsBinCleanerHandlerProcessorForGetConfiguration : AbstractDatabaseHandlerProcessorForGetConfiguration<DatabaseRequestHandler, DocumentsOperationContext, RevisionsBinConfiguration>
    {
        public RevisionsBinCleanerHandlerProcessorForGetConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RevisionsBinConfiguration GetConfiguration()
        {
            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                RevisionsBinConfiguration configuration;
                using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                {
                    configuration = rawRecord?.RevisionsBin;
                }
                return configuration;
            }
        }
    }
}
