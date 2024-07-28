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
using static Raven.Server.Documents.Revisions.RevisionsStorage;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForDeleteRevisions : AbstractRevisionsHandlerProcessorForDeleteRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForDeleteRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<long> DeleteRevisionsAsync(DeleteRevisionsRequest request, OperationCancelToken token)
        {
            if (request.RevisionsChangeVectors.IsNullOrEmpty() == false)
                return DeleteRevisionsByChangeVectorAsync(request.DocumentId, request.RevisionsChangeVectors);

            return DeleteRevisionsByDocumentIdAsync(request.DocumentId, request.MaxDeletes,
                request.After, request.Before);
        }

        private async Task<long> DeleteRevisionsByChangeVectorAsync(string id, List<string> cvs)
        {
            var cmd = new DeleteRevisionsByChangeVectorMergedCommand(id, cvs);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
            return cmd.Result.HasValue ? cmd.Result.Value : 0;
        }

        private async Task<long> DeleteRevisionsByDocumentIdAsync(string id, long maxDeletes, DateTime? after, DateTime? before)
        {
            var cmd = new DeleteRevisionsByDocumentIdMergedCommand(id, maxDeletes, after, before);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
            return cmd.Result.HasValue ? cmd.Result.Value : 0;
        }
    }
}
