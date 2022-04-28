using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForDeleteRevisions : AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override async ValueTask DeleteRevisionsAsync(DocumentsOperationContext _, string[] documentIds)
        {
            var cmd = new DeleteRevisionsCommand(documentIds, RequestHandler.Database);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

        internal class DeleteRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
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

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new DeleteRevisionsCommandDto {Ids = _ids};
            }
        }

        internal class DeleteRevisionsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<DeleteRevisionsCommand>
        {
            public string[] Ids;

            public DeleteRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new DeleteRevisionsCommand(Ids, database);
                return command;
            }
        }
    }
}
