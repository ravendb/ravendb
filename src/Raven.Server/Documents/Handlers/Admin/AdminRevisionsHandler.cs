// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Exceptions.Revisions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", "/databases/*/admin/revisions?id={documentId:string|multiple}",
            RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task DeleteRevisionsFor()
        {
            var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
            if (revisionsStorage.Configuration == null)
                throw new RevisionsDisabledException();

            var ids = GetStringValuesQueryString("id");

            var cmd = new DeleteRevisionsCommand(ids, Database);
            await Database.TxMerger.Enqueue(cmd);
            NoContentStatus();
        }

        private class DeleteRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly StringValues _ids;
            private readonly DocumentDatabase _database;

            public DeleteRevisionsCommand(StringValues ids, DocumentDatabase database)
            {
                _ids = ids;
                _database = database;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var id in _ids)
                {
                    _database.DocumentsStorage.RevisionsStorage.DeleteRevisionsFor(context, id);
                }
                return 1;
            }
        }
    }
}