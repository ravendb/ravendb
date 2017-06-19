using System;
using Raven.Client.Documents.Exceptions.Versioning;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Versioning
{
    public class VersioningOperations
    {
        private readonly DocumentDatabase _database;

        public VersioningOperations(DocumentDatabase database)
        {
            _database = database;
        }

        public void DeleteRevisionsBefore(string collection, DateTime time)
        {
            var versioningStorage = _database.DocumentsStorage.VersioningStorage;
            if (versioningStorage.Configuration == null)
                throw new VersioningDisabledException();
            _database.TxMerger.Enqueue(new DeleteRevisionsBeforeCommand(collection, time, _database)).Wait();
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
                _database.DocumentsStorage.VersioningStorage.DeleteRevisionsBefore(context, _collection, _time);
                return 1;
            }
        }
    }
}