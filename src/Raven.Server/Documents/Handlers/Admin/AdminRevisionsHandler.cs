// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.TransactionMerger.Commands;
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
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                var cmd = new DeleteRevisionsCommand(parameters.DocumentIds, Database);
                await Database.TxMerger.Enqueue(cmd);
                NoContentStatus();
            }
        }

        internal class DeleteRevisionsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Microsoft.Extensions.Primitives.StringValues _ids;
            private readonly DocumentDatabase _database;

            public DeleteRevisionsCommand(string[] ids, DocumentDatabase database)
            {
                _ids = ids;
                _database = database;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                foreach (var id in _ids)
                {
                    _database.DocumentsStorage.RevisionsStorage.DeleteRevisionsFor(context, id);
                }
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new DeleteRevisionsCommandDto
                {
                    Ids = _ids
                };
            }
        }

        public class Parameters
        {
            public string[] DocumentIds { get; set; }
        }
    }

    internal class DeleteRevisionsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AdminRevisionsHandler.DeleteRevisionsCommand>
    {
        public string[] Ids;

        public AdminRevisionsHandler.DeleteRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new AdminRevisionsHandler.DeleteRevisionsCommand(Ids, database);
            return command;
        }
    }
}
