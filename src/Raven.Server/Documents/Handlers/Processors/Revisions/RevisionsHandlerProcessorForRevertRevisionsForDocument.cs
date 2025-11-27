using System;
using System.Collections.Generic;
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

        protected override Task RevertDocumentsAsync(Dictionary<string, string> idToChangeVector, OperationCancelToken token)
        {
            if(RequestHandler.Database.SchemaValidatorCache.Disabled == false)
                throw new InvalidOperationException("Reverting documents to revisions is not allowed when Schema Validation is enabled. Please disable Schema Validation and try again.");
                
            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.RevertDocumentsToRevisionsAsync(idToChangeVector, token);
        }
    }
}
