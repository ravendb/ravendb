using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.Revisions.RevisionsStorage;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal sealed class AdminRevisionsHandlerProcessorForDeleteRevisions : AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<long> DeleteRevisionsAsync(DeleteRevisionsOperation.Parameters request, OperationCancelToken token)
        {
            if (request.RevisionsChangeVectors.IsNullOrEmpty() == false)
                return DeleteRevisionsByChangeVectorAsync(request.DocumentIds.First(), request.RevisionsChangeVectors, request.IncludeForceCreated);

            return DeleteRevisionsByDocumentIdAsync(request.DocumentIds, request.After, request.Before, request.IncludeForceCreated, token);
        }

        private async Task<long> DeleteRevisionsByChangeVectorAsync(string id, List<string> cvs, bool includeForceCreated)
        {
            var cmd = new DeleteRevisionsByChangeVectorMergedCommand(id, cvs, includeForceCreated);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
            return cmd.Result.HasValue ? cmd.Result.Value : 0;
        }

        private async Task<long> DeleteRevisionsByDocumentIdAsync(IEnumerable<string> ids, DateTime? after, DateTime? before, bool includeForceCreated, OperationCancelToken token)
        {
            var deleted = 0L;
            var moreWork = false;

            do
            {
                var cmd = new DeleteRevisionsByDocumentIdMergedCommand(ids, after, before, includeForceCreated);
                await RequestHandler.Database.TxMerger.Enqueue(cmd);

                if (cmd.Result.HasValue)
                {
                    deleted += cmd.Result.Value.Deleted;
                    moreWork = cmd.Result.Value.MoreWork;
                }
            } while(moreWork);

            return deleted;
        }

    }
}
