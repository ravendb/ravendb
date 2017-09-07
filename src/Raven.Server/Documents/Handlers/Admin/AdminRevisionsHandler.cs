// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteRevisionsFor()
        {
            var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
            if (revisionsStorage.Configuration == null)
                throw new RevisionsDisabledException();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                var cmd = new DeleteRevisionsCommand(parameters.DocumentIds, Database);
                await Database.TxMerger.Enqueue(cmd);
                NoContentStatus();
            }
        }

        private class DeleteRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly StringValues _ids;
            private readonly DocumentDatabase _database;

            public DeleteRevisionsCommand(string[] ids, DocumentDatabase database)
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

        public class Parameters
        {
            public string[] DocumentIds { get; set; }
        }
    }
}
