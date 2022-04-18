using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForGetRevisionsConflictsConfiguration : AbstractRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForGetRevisionsConflictsConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override RevisionsCollectionConfiguration GetRevisionsConflicts(DocumentsOperationContext context)
        {
            using (context.OpenReadTransaction())
            using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                return rawRecord?.RevisionsForConflicts;
            }
        }
    }
}
