using System.Runtime.ExceptionServices;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.TransactionCommands
{
    public class DeleteDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly string _id;
        private readonly string _expectedChangeVector;
        private readonly DocumentDatabase _database;

        public DocumentsStorage.DeleteOperationResult? DeleteResult;

        public DeleteDocumentCommand(string id, string changeVector, DocumentDatabase database)
        {
            _id = id;
            _expectedChangeVector = changeVector;
            _database = database;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            DeleteResult = _database.DocumentsStorage.Delete(context, _id, _expectedChangeVector);
            return 1;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new DeleteDocumentCommandDto()
            {
                Id = _id,
                ChangeVector = _expectedChangeVector,
            };
        }
    }

    public class DeleteDocumentCommandDto : TransactionOperationsMerger.IReplayableCommandDto<DeleteDocumentCommand>
    {
        public string Id { get; set; }
        public string ChangeVector { get; set; }
        public bool CatchConcurrencyErrors { get; set; }

        public DeleteDocumentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new DeleteDocumentCommand(Id, ChangeVector, database);
        }
    }
}
