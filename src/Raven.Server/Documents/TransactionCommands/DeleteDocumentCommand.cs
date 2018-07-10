using System.Runtime.ExceptionServices;
using System.Runtime.Serialization;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TransactionCommands
{
    public class DeleteDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand, TransactionOperationsMerger.IRecordableCommand
    {
        private const string IdKey = "Id";
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

        public DynamicJsonValue Serialize()
        {
            var json = new DynamicJsonValue
            {
                [IdKey] = _id
            };

            return json;
        }

        public static DeleteDocumentCommand Deserialize(BlittableJsonReaderObject reader, DocumentDatabase database)
        {
            if (!reader.TryGet(IdKey, out string id))
            {
                throw new SerializationException($"Can't read {IdKey} of {nameof(DeleteDocumentCommand)}");
            }

            return new DeleteDocumentCommand(id, null, database);
        }
    }
}
