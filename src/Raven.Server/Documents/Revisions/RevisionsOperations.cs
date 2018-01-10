using System;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Revisions
{
    public class RevisionsOperations
    {
        private readonly DocumentDatabase _database;

        public RevisionsOperations(DocumentDatabase database)
        {
            _database = database;
        }

        public void DeleteRevisionsBefore(string collection, DateTime time)
        {
            var revisionsStorage = _database.DocumentsStorage.RevisionsStorage;
            if (revisionsStorage.Configuration == null)
                throw new RevisionsDisabledException();
            _database.TxMerger.Enqueue(new DeleteRevisionsBeforeCommand(collection, time, _database)).GetAwaiter().GetResult();
        }

        private class DeleteRevisionsBeforeCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly string _collection;
            private readonly DateTime _time;
            private readonly DocumentDatabase _database;

            public DeleteRevisionsBeforeCommand(string collection, DateTime time, DocumentDatabase database)
            {
                _collection = collection;
                _time = time;
                _database = database;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                _database.DocumentsStorage.RevisionsStorage.DeleteRevisionsBefore(context, _collection, _time);
                return 1;
            }
        }
    }
}
