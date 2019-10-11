using System;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public class DeleteReduceOutputDocumentsCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly DocumentDatabase _database;
        private readonly string _documentsPrefix;
        private readonly int _batchSize;

        public DeleteReduceOutputDocumentsCommand(DocumentDatabase database, string documentsPrefix, int batchSize)
        {
            if (OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand.IsOutputDocumentPrefix(documentsPrefix) == false)
                throw new ArgumentException($"Invalid prefix to delete: {documentsPrefix}", nameof(documentsPrefix));

            _database = database;
            _documentsPrefix = documentsPrefix;
            _batchSize = batchSize;
        }

        public long DeleteCount { get; set; }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            var deleteResults = _database.DocumentsStorage.DeleteDocumentsStartingWith(context, _documentsPrefix, _batchSize);

            DeleteCount = deleteResults.Count;

            return deleteResults.Count;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new DeleteReduceOutputDocumentsCommandDto
            {
                DocumentsPrefix = _documentsPrefix,
                BatchSize = _batchSize,
            };
        }
    }

    public class DeleteReduceOutputDocumentsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<DeleteReduceOutputDocumentsCommand>
    {
        public string DocumentsPrefix;
        public int BatchSize;

        public DeleteReduceOutputDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new DeleteReduceOutputDocumentsCommand(database, DocumentsPrefix, BatchSize);

            return command;
        }
    }
}
