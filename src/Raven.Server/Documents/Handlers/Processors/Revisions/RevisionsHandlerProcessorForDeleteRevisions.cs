using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

        protected override async Task DeleteRevisions(DeleteRevisionsRequest request, OperationCancelToken token)
        {
            await RequestHandler.Database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyAsync(request.DocumentIds, request.MaxDeletes);
            await RequestHandler.Database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyAsync(request.RevisionsChangeVecotors, request.MaxDeletes);
        }
    }
}
