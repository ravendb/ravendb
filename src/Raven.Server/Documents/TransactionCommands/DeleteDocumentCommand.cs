using System.Runtime.ExceptionServices;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;

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

        public override int Execute(DocumentsOperationContext context)
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
    }
}
