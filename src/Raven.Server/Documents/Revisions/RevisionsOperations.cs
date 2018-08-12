using System;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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

        internal class DeleteRevisionsBeforeCommand : TransactionOperationsMerger.MergedTransactionCommand, TransactionOperationsMerger.IRecordableCommand
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

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                _database.DocumentsStorage.RevisionsStorage.DeleteRevisionsBefore(context, _collection, _time);
                return 1;
            }

            public TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new DeleteRevisionsBeforeCommandDto
                {
                    Collection = _collection,
                    //Todo To consider what need to be the result because while replying the revisions are newer then the date of delete before 
                    Time = _time
                };
            }
        }
    }

    internal class DeleteRevisionsBeforeCommandDto : TransactionOperationsMerger.IReplayableCommandDto<RevisionsOperations.DeleteRevisionsBeforeCommand>
    {
        public string Collection;
        public DateTime Time;

        public RevisionsOperations.DeleteRevisionsBeforeCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new RevisionsOperations.DeleteRevisionsBeforeCommand(Collection, Time, database);
            return command;
        }
    }
}
