// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
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
            var deleteOnlyForceCreated = GetBoolValueQueryString("onlyForceCreated", required: false) ?? false;

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                var cmd = new DeleteRevisionsCommand(parameters.DocumentIds, Database, deleteOnlyForceCreated);
                await Database.TxMerger.Enqueue(cmd);
                NoContentStatus();
            }
        }

        internal class DeleteRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Microsoft.Extensions.Primitives.StringValues _ids;
            private readonly DocumentDatabase _database;
            private readonly bool _deleteOnlyForceCreated;

            public DeleteRevisionsCommand(string[] ids, DocumentDatabase database, bool deleteOnlyForceCreated)
            {
                _ids = ids;
                _database = database;
                _deleteOnlyForceCreated = deleteOnlyForceCreated;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                foreach (var id in _ids)
                {
                    _database.DocumentsStorage.RevisionsStorage.DeleteAllRevisionsFor(context, id, _deleteOnlyForceCreated);
                }
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
            {
                return new DeleteRevisionsCommandDto(_ids, _deleteOnlyForceCreated);
            }
        }

        public class Parameters
        {
            public string[] DocumentIds { get; set; }
        }
    }

    internal class DeleteRevisionsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AdminRevisionsHandler.DeleteRevisionsCommand>
    {
        private readonly string[] _ids;
        private readonly bool _deleteOnlyForceCreated;

        public DeleteRevisionsCommandDto(string[] ids, bool deleteOnlyForceCreated)
        {
            _ids = ids;
            _deleteOnlyForceCreated = deleteOnlyForceCreated;
        }

        public AdminRevisionsHandler.DeleteRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new AdminRevisionsHandler.DeleteRevisionsCommand(_ids, database, _deleteOnlyForceCreated);
            return command;
        }
    }
}
