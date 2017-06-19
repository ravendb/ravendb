// -----------------------------------------------------------------------
//  <copyright file="VersioningHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Exceptions.Versioning;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminVersioningHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", "/databases/*/admin/revisions?id={documentId:string|multiple}")]
        public async Task DeleteRevisionsFor()
        {
            var versioningStorage = Database.DocumentsStorage.VersioningStorage;
            if (versioningStorage.Configuration == null)
                throw new VersioningDisabledException();

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
                    _database.DocumentsStorage.VersioningStorage.DeleteRevisionsFor(context, id);
                }
                return 1;
            }
        }
    }
}