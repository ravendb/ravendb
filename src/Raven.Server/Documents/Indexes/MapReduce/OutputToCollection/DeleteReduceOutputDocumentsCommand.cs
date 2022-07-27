using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class DeleteReduceOutputDocumentsCommand : OutputReduceAbstractCommand
    {
        private readonly string _documentsPrefix;
        private readonly int _batchSize;
        private readonly string _originalPattern = null;
        private readonly OutputReferencesPattern _originalPatternForReduceOutputReferences = null;

        public DeleteReduceOutputDocumentsCommand(DocumentDatabase database, string documentsPrefix, string originalPattern, int batchSize) : base(database)
        {
            if (OutputReduceToCollectionCommand.IsOutputDocumentPrefix(documentsPrefix) == false)
                throw new ArgumentException($"Invalid prefix to delete: {documentsPrefix}", nameof(documentsPrefix));

            _documentsPrefix = documentsPrefix;
            _batchSize = batchSize;

            if (string.IsNullOrEmpty(originalPattern) == false)
            {
                _originalPattern = originalPattern;
                _originalPatternForReduceOutputReferences = new OutputReferencesPattern(database, originalPattern);
            }
        }

        public long DeleteCount { get; set; }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            List<DocumentsStorage.DeleteOperationResult> deleteResults;
            Dictionary<string, HashSet<string>> idsToDeleteByReferenceDocumentId = null;

            if (_originalPatternForReduceOutputReferences == null)
            {
                deleteResults = _database.DocumentsStorage.DeleteDocumentsStartingWith(context, _documentsPrefix, _batchSize);
            }
            else
            {
                idsToDeleteByReferenceDocumentId = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

                deleteResults = _database.DocumentsStorage.DeleteDocumentsStartingWith(context, _documentsPrefix, _batchSize,
                    doc => RetrieveReferenceDocumentsToCleanup(doc, idsToDeleteByReferenceDocumentId));
            }

            DeleteCount = deleteResults.Count;

            if (idsToDeleteByReferenceDocumentId != null) 
                CleanupReferenceDocuments(context, idsToDeleteByReferenceDocumentId);

            return deleteResults.Count;
        }

        private void RetrieveReferenceDocumentsToCleanup(Document doc, Dictionary<string, HashSet<string>> idsToDeleteByReferenceDocumentId)
        {
            using (_originalPatternForReduceOutputReferences.BuildReferenceDocumentId(out var referenceDocIdBuilder))
            {
                foreach (string field in referenceDocIdBuilder.PatternFields)
                {
                    var fieldValue = doc.Data[field];

                    fieldValue = TypeConverter.ConvertForIndexing(fieldValue);

                    referenceDocIdBuilder.Add(field, fieldValue);
                }

                var referenceId = referenceDocIdBuilder.GetId();

                if (idsToDeleteByReferenceDocumentId.TryGetValue(referenceId, out var values) == false)
                {
                    values = new HashSet<string>(1, StringComparer.OrdinalIgnoreCase);

                    idsToDeleteByReferenceDocumentId.Add(referenceId, values);
                }

                values.Add(doc.Id);
            }
        }

        private void CleanupReferenceDocuments(DocumentsOperationContext context, Dictionary<string, HashSet<string>> idsToDeleteByReferenceDocumentId)
        {
            foreach (var reduceReferenceIdToReduceOutputIds in idsToDeleteByReferenceDocumentId)
            {
                using (var referenceDocument = _database.DocumentsStorage.Get(context, reduceReferenceIdToReduceOutputIds.Key))
                {
                    if (referenceDocument == null)
                        continue;

                    if (referenceDocument.Data.TryGet(nameof(OutputReduceToCollectionReference.ReduceOutputs), out BlittableJsonReaderArray ids) == false)
                        ThrowIdsPropertyNotFound(referenceDocument.Id);

                    var idsToRemove = reduceReferenceIdToReduceOutputIds.Value;

                    if (ids.Modifications == null)
                        ids.Modifications = new DynamicJsonArray();

                    var indexesToRemove = new List<int>();

                    for (int i = ids.Length - 1; i >= 0; i--)
                    {
                        var id = ids[i].ToString();

                        if (idsToRemove.Contains(id))
                            indexesToRemove.Add(i);

                        if (idsToRemove.Count == indexesToRemove.Count)
                            break;
                    }

                    foreach (int toRemove in indexesToRemove)
                    {
                        ids.Modifications.RemoveAt(toRemove);
                    }

                    using (var doc = context.ReadObject(referenceDocument.Data, referenceDocument.Id))
                    {
                        if (doc.TryGet(nameof(OutputReduceToCollectionReference.ReduceOutputs), out BlittableJsonReaderArray updatedIds) == false)
                            ThrowIdsPropertyNotFound(referenceDocument.Id);

                        if (updatedIds.Length == 0)
                            ArtificialDelete(context, referenceDocument.Id);
                        else
                            ArtificialPut(context, referenceDocument.Id, doc);
                    }
                }
            }
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new DeleteReduceOutputDocumentsCommandDto
            {
                DocumentsPrefix = _documentsPrefix,
                BatchSize = _batchSize,
                OriginalPattern = _originalPattern
            };
        }

        private static void ThrowIdsPropertyNotFound(string id)
        {
            throw new InvalidOperationException($"Property {nameof(OutputReduceToCollectionReference.ReduceOutputs)} was not found in document: {id}");
        }
    }

    public class DeleteReduceOutputDocumentsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<DeleteReduceOutputDocumentsCommand>
    {
        public string DocumentsPrefix;
        public string OriginalPattern;
        public int BatchSize;

        public DeleteReduceOutputDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new DeleteReduceOutputDocumentsCommand(database, DocumentsPrefix, OriginalPattern, BatchSize);

            return command;
        }
    }
}
