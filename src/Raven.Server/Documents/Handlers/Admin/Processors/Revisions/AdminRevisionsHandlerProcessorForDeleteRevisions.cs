using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForDeleteRevisions : AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override async ValueTask DeleteRevisionsAsync(DocumentsOperationContext _, string[] documentIds, bool includeForceCreated,
            OperationCancelToken token)
        {

            DeleteRevisionsCommand cmd;
            do
            {
                token.Delay();
                cmd = new DeleteRevisionsCommand(documentIds, RequestHandler.Database, includeForceCreated, token);
                await RequestHandler.Database.TxMerger.Enqueue(cmd);
            } while (cmd.MoreWork);
        }

        internal class DeleteRevisionsCommand : DocumentMergedTransactionCommand
        {
            private readonly Microsoft.Extensions.Primitives.StringValues _ids;
            private readonly DocumentDatabase _database;
            private readonly bool _includeForceCreated;
            private readonly OperationCancelToken _token;

            public bool MoreWork;

            public DeleteRevisionsCommand(string[] ids, DocumentDatabase database, bool includeForceCreated, OperationCancelToken token)
            {
                _ids = ids;
                _database = database;
                _includeForceCreated = includeForceCreated;
                _token = token;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var skipForceCreated = _includeForceCreated == false;
                MoreWork = false;
                foreach (var id in _ids)
                {
                    _token.ThrowIfCancellationRequested();
                    _database.DocumentsStorage.RevisionsStorage.DeleteAllRevisionsFor(context, id, skipForceCreated, ref MoreWork);
                }

                return 1;
            }


            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return new DeleteRevisionsCommandDto() { Ids = _ids, IncludeForceCreated = _includeForceCreated };
            }
        }

        internal class DeleteRevisionsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsCommand>
        {
            public string[] Ids;
            public bool IncludeForceCreated;

            public DeleteRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new DeleteRevisionsCommand(Ids, database, IncludeForceCreated, OperationCancelToken.None);
                return command;
            }
        }
    }
}
