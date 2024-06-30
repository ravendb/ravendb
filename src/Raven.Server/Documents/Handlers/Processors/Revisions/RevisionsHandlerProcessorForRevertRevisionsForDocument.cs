using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForRevertRevisionsForDocument : AbstractRevisionsHandlerProcessorForRevertRevisionsForDocument<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForRevertRevisionsForDocument([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task RevertDocuments(Dictionary<string, string> idToChangeVector, OperationCancelToken token)
        {
            var revisionsStorage = RequestHandler.Database.DocumentsStorage.RevisionsStorage;

            revisionsStorage.VerifyRevisionsIdsAndChangeVectors(idToChangeVector);

            return revisionsStorage.RevertDocumentsToRevisions(changeVectors: idToChangeVector.Values.ToList(), token);
        }
    }
}
