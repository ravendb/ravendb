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
        private readonly bool _catchConcurrencyErrors;

        public ExceptionDispatchInfo ExceptionDispatchInfo;

        public DocumentsStorage.DeleteOperationResult? DeleteResult;

        public DeleteDocumentCommand(string id, string changeVector, DocumentDatabase database, bool catchConcurrencyErrors = false)
        {
            _id = id;
            _expectedChangeVector = changeVector;
            _database = database;
            _catchConcurrencyErrors = catchConcurrencyErrors;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            try
            {
                DeleteResult = _database.DocumentsStorage.Delete(context, _id, _expectedChangeVector);
            }
            catch (ConcurrencyException e)
            {
                if (_catchConcurrencyErrors == false)
                    throw;

                ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return 1;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new DeleteDocumentCommandDto()
            {
                Id = _id,
                ChangeVector = _expectedChangeVector,
                CatchConcurrencyErrors = _catchConcurrencyErrors
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
            return new DeleteDocumentCommand(Id, ChangeVector, database, CatchConcurrencyErrors);
        }
    }
}
