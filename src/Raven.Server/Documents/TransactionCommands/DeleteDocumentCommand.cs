using System.Runtime.ExceptionServices;
using Raven.Server.ServerWide.Context;
using Voron.Exceptions;

namespace Raven.Server.Documents.TransactionCommands
{
    public class DeleteDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly string _id;
        private readonly long? _expectedEtag;
        private readonly DocumentDatabase _database;
        private readonly bool _catchConcurrencyErrors;

        public ExceptionDispatchInfo ExceptionDispatchInfo;

        public DocumentsStorage.DeleteOperationResult? DeleteResult;

        public DeleteDocumentCommand(string id, long? etag, DocumentDatabase database, bool catchConcurrencyErrors = false)
        {
            _id = id;
            _expectedEtag = etag;
            _database = database;
            _catchConcurrencyErrors = catchConcurrencyErrors;
        }

        public override int Execute(DocumentsOperationContext context)
        {
            try
            {
                DeleteResult = _database.DocumentsStorage.Delete(context, _id, _expectedEtag);
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