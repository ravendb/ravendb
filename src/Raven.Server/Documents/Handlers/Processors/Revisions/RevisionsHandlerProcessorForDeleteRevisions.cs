using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForDeleteRevisions : AbstractRevisionsHandlerProcessorForDeleteRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForDeleteRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<long> DeleteRevisions(DeleteRevisionsRequest request, OperationCancelToken token)
        {
            if(request.RevisionsChangeVecotors.IsNullOrEmpty()==false)
                return RequestHandler.Database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyAsync(request.RevisionsChangeVecotors);

            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyAsync(request.DocumentId, request.MaxDeletes, request.After, request.Before);
        }
    }
}
