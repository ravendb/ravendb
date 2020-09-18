using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceToCollectionCommandBatcher : IDisposable
    {
        private static int BatchSize = PlatformDetails.Is32Bits == false
            ? 4096
            : 1024;

        private readonly DocumentDatabase _database;
        private readonly string _outputReduceToCollection;
        private readonly long? _reduceOutputIndex;
        private readonly OutputReferencesPattern _patternForReduceOutputReferences;

        private readonly JsonOperationContext _indexContext;
        private readonly TransactionHolder _indexWriteTxHolder;
        private readonly MapReduceIndex _index;

        private readonly List<string> _reduceKeyHashesToDelete = new List<string>();

        private readonly Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocId)>> _reduceDocuments =
            new Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocId)>>();

        public OutputReduceToCollectionCommandBatcher(
            DocumentDatabase database,
            string outputReduceToCollection,
            long? reduceOutputIndex,
            OutputReferencesPattern patternForReduceOutputReferences,
            MapReduceIndex index,
            JsonOperationContext indexContext,
            TransactionHolder indexWriteTxHolder)
        {
            _database = database;
            _outputReduceToCollection = outputReduceToCollection;
            _reduceOutputIndex = reduceOutputIndex;
            _patternForReduceOutputReferences = patternForReduceOutputReferences;
            _index = index;
            _indexContext = indexContext;
            _indexWriteTxHolder = indexWriteTxHolder;
        }

        public void AddReduce(string reduceKeyHash, object reduceObject, IndexingStatsScope stats)
        {
            if (_reduceDocuments.TryGetValue(reduceKeyHash, out var outputs) == false)
            {
                outputs = new List<(BlittableJsonReaderObject, string)>(1);
                _reduceDocuments.Add(reduceKeyHash, outputs);
            }

            if (_index.OutputReduceToCollectionPropertyAccessor == null)
                _index.OutputReduceToCollectionPropertyAccessor = PropertyAccessor.Create(reduceObject.GetType(), reduceObject);

            var reduceObjectJson = GenerateReduceOutput(reduceObject, stats, out string referenceDocumentId);

            outputs.Add((reduceObjectJson, referenceDocumentId));
        }

        public void DeleteReduce(string reduceKeyHash)
        {
            _reduceKeyHashesToDelete.Add(reduceKeyHash);

            if (_reduceDocuments.Remove(reduceKeyHash, out var outputs))
            {
                foreach (var bjro in outputs)
                {
                    bjro.Json.Dispose();
                }
            }
        }

        public IEnumerable<OutputReduceToCollectionCommand> CreateCommands()
        {
            var batchSize = 0;
            OutputReduceToCollectionCommand command = null;
            foreach (var reduceKeyHashToDelete in _reduceKeyHashesToDelete)
            {
                command ??= CreateCommand();

                if (command.DeleteReduce(reduceKeyHashToDelete) == false)
                    continue;

                batchSize++;

                if (batchSize == BatchSize)
                {
                    yield return command;
                    batchSize = 0;
                    command = null;
                }
            }

            foreach (var kvp in _reduceDocuments)
            {
                var reduceKeyHash = kvp.Key;

                foreach (var output in kvp.Value)
                {
                    command ??= CreateCommand();
                    command.AddReduce(reduceKeyHash, output);

                    batchSize++;

                    if (batchSize == BatchSize)
                    {
                        yield return command;
                        batchSize = 0;
                        command = null;
                    }
                }
            }

            if (command != null)
                yield return command;

            OutputReduceToCollectionCommand CreateCommand()
            {
                return new OutputReduceToCollectionCommand(_database, _outputReduceToCollection, _reduceOutputIndex, _patternForReduceOutputReferences, _index, _indexWriteTxHolder);
            }
        }

        private BlittableJsonReaderObject GenerateReduceOutput(object reduceObject, IndexingStatsScope stats, out string referenceDocumentId)
        {
            var djv = new DynamicJsonValue();

            referenceDocumentId = null;

            OutputReferencesPattern.DocumentIdBuilder referenceDocIdBuilder = null;

            using (_patternForReduceOutputReferences?.BuildReferenceDocumentId(out referenceDocIdBuilder))
            {
                var patternPropertiesAddedSuccessfully = true;

                foreach (var property in _index.OutputReduceToCollectionPropertyAccessor.GetPropertiesInOrder(reduceObject))
                {
                    var value = property.Value;
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _indexContext);

                    if (referenceDocIdBuilder?.ContainsField(property.Key) == true)
                    {
                        try
                        {
                            referenceDocIdBuilder.Add(property.Key, property.Value);
                        }
                        catch (Exception e)
                        {
                            patternPropertiesAddedSuccessfully = false;

                            if (stats != null) // should never be null
                                stats.AddReduceError($"Failed to build document ID based on provided pattern for output to collection references. {e}");
                        }
                    }
                }

                if (referenceDocIdBuilder != null && patternPropertiesAddedSuccessfully)
                {
                    try
                    {
                        referenceDocumentId = referenceDocIdBuilder.GetId();
                    }
                    catch (Exception e)
                    {
                        if (stats != null) // should never be null
                            stats.AddReduceError($"Failed to build document ID based on provided pattern for output to collection references. {e}");
                    }
                }
            }

            djv[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = _outputReduceToCollection
            };

            return _indexContext.ReadObject(djv, "output-of-reduce-doc", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public void Dispose()
        {
            foreach (var r in _reduceDocuments)
            {
                foreach (var doc in r.Value)
                {
                    doc.Json.Dispose();
                }
            }
        }
    }

    public class OutputReduceToCollectionCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private const string MultipleOutputsForSameReduceKeyHashSeparator = "/";

        private readonly DocumentDatabase _database;
        private readonly string _outputReduceToCollection;
        private readonly long? _reduceOutputIndex;
        private readonly OutputReferencesPattern _patternForReduceOutputReferences;
        private readonly HashSet<string> _reduceKeyHashesToDelete = new HashSet<string>();
        private readonly OutputReduceToCollectionReferencesCommand _outputToCollectionReferences;

        private readonly Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocId)>> _reduceDocuments =
            new Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocId)>>();

        private readonly Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>> _reduceDocumentsForReplayTransaction =
            new Dictionary<string, List<(BlittableJsonReaderObject, string)>>();

        private readonly TransactionHolder _indexWriteTxHolder;

        public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex,
            OutputReferencesPattern patternForReduceOutputReferences, MapReduceIndex index, TransactionHolder indexWriteTxHolder)
            : this(database, outputReduceToCollection, reduceOutputIndex)
        {
            _patternForReduceOutputReferences = patternForReduceOutputReferences;
            _indexWriteTxHolder = indexWriteTxHolder;

            if (_patternForReduceOutputReferences != null)
                _outputToCollectionReferences = new OutputReduceToCollectionReferencesCommand(index, outputReduceToCollection, _patternForReduceOutputReferences.ReferencesCollectionName);
        }

        /// <summary>
        ///This constructor should be used for replay transaction operations only
        /// </summary>
        internal OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex,
            Dictionary<string, List<(BlittableJsonReaderObject, string)>> reduceDocuments)
            : this(database, outputReduceToCollection, reduceOutputIndex)
        {
            _reduceDocumentsForReplayTransaction = reduceDocuments;
        }

        private OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex)
        {
            _database = database;
            _outputReduceToCollection = outputReduceToCollection;
            _reduceOutputIndex = reduceOutputIndex;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_reduceDocumentsForReplayTransaction != null && _reduceDocumentsForReplayTransaction.Count > 0)
            {
                ProcessReduceDocumentsForReplayTransaction(context);
                return _reduceDocumentsForReplayTransaction.Count;
            }

            foreach (var reduceKeyHash in _reduceKeyHashesToDelete)
            {
                var id = GetOutputDocumentKey(reduceKeyHash);

                // we could have just used 'Delete' but this method is also using the 'Get' to determine the collection name
                var currentDocument = _database.DocumentsStorage.Get(context, id, DocumentFields.Data);
                if (currentDocument != null)
                {
                    // apply this optimization only if we don't have hash collisions
                    if (_reduceDocuments.TryGetValue(reduceKeyHash, out var newReduceDocuments) &&
                        newReduceDocuments.Count == 1 &&
                        DocumentsBinaryEqual(currentDocument.Data, newReduceDocuments[0].Json))
                    {
                        // same document, nothing to do
                        _reduceDocuments.Remove(reduceKeyHash);
                        continue;
                    }

                    _database.DocumentsStorage.Delete(context, id, null);
                    _outputToCollectionReferences?.Delete(id);
                }
                else
                {
                    // document with a given reduce key hash doesn't exist
                    // let's try to delete documents by reduce key hash prefix in case we got hash collision

                    var prefix = GetOutputDocumentKeyForSameReduceKeyHashPrefix(reduceKeyHash);

                    if (_outputToCollectionReferences != null)
                    {
                        var docs = _database.DocumentsStorage.GetDocumentsStartingWith(context, prefix, null, null, null, 0, long.MaxValue);

                        foreach (var doc in docs)
                        {
                            using (doc)
                            {
                                _outputToCollectionReferences.Delete(doc.Id);
                            }
                        }
                    }

                    _database.DocumentsStorage.DeleteDocumentsStartingWith(context, prefix);
                }
            }

            foreach (var output in _reduceDocuments)
            {
                for (var i = 0; i < output.Value.Count; i++) // we have hash collision so there might be multiple outputs for the same reduce key hash
                {
                    var id = output.Value.Count == 1 ? GetOutputDocumentKey(output.Key) : GetOutputDocumentKeyForSameReduceKeyHash(output.Key, i);
                    var item = output.Value[i];
                    var doc = item.Json;

                    _database.DocumentsStorage.Put(context, id, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                    context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(id, doc.Size);

                    if (item.ReferenceDocId != null)
                        _outputToCollectionReferences?.Add(item.ReferenceDocId, id);
                }
            }

            if (_outputReduceToCollection != null)
            {
                using (_indexWriteTxHolder.AcquireTransaction(out var writeTransaction))
                {
                    // we must not let index write transaction to run concurrently so we wait for index writer commit
                    _outputToCollectionReferences?.Execute(context, writeTransaction);
                }
            }

            return _reduceDocuments.Count;
        }

        private static unsafe bool DocumentsBinaryEqual(BlittableJsonReaderObject oldDocument, BlittableJsonReaderObject newDocument)
        {
            return oldDocument.Size == newDocument.Size &&
                   Sparrow.Memory.CompareInline(oldDocument.BasePointer, newDocument.BasePointer, oldDocument.Size) == 0;
        }

        private void ProcessReduceDocumentsForReplayTransaction(DocumentsOperationContext context)
        {
            foreach (var output in _reduceDocumentsForReplayTransaction)
            {
                for (var i = 0; i < output.Value.Count; i++) // we have hash collision so there might be multiple outputs for the same reduce key hash
                {
                    var key = output.Value.Count == 1 ? GetOutputDocumentKey(output.Key) : GetOutputDocumentKeyForSameReduceKeyHash(output.Key, i);
                    var item = output.Value[i];

                    using (item.Json) // TODO arek item.ReferenceDocumentId
                    {
                        _database.DocumentsStorage.Put(context, key, null, item.Json, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(key, item.Json.Size);
                    }
                }
            }
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            var dto = new OutputReduceToCollectionCommandDto
            {
                OutputReduceToCollection = _outputReduceToCollection,
                ReduceOutputIndex = _reduceOutputIndex
            };

            dto.ReduceDocuments = _reduceDocuments;

            return dto;
        }

        public void AddReduce(string reduceKeyHash, (BlittableJsonReaderObject Json, string ReferenceDocId) output)
        {
            if (_reduceDocuments.TryGetValue(reduceKeyHash, out var outputs) == false)
                _reduceDocuments[reduceKeyHash] = outputs = new List<(BlittableJsonReaderObject Json, string ReferenceDocId)>();

            outputs.Add(output);
        }

        public bool DeleteReduce(string reduceKeyHash)
        {
            return _reduceKeyHashesToDelete.Add(reduceKeyHash);
        }

        public static bool IsOutputDocumentPrefix(string prefix)
        {
            var parts = prefix.Split("/", StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length < 2)
                return false;

            string numeric = parts.Last();

            if (long.TryParse(numeric, out _) == false)
                return false;

            return true;
        }

        public static string GetOutputDocumentPrefix(string collectionName, long reduceOutputIndex)
        {
            return collectionName + "/" + reduceOutputIndex + "/";
        }

        private string GetOutputDocumentKey(string reduceKeyHash)
        {
            if (_reduceOutputIndex != null)
                return GetOutputDocumentPrefix(_outputReduceToCollection, _reduceOutputIndex.Value) + reduceKeyHash;

            return _outputReduceToCollection + "/" + reduceKeyHash;
        }

        private string GetOutputDocumentKeyForSameReduceKeyHash(string reduceKeyHash, int outputNumber)
        {
            return GetOutputDocumentKeyForSameReduceKeyHashPrefix(reduceKeyHash) + outputNumber;
        }

        private string GetOutputDocumentKeyForSameReduceKeyHashPrefix(string reduceKeyHash)
        {
            return GetOutputDocumentKey(reduceKeyHash) + MultipleOutputsForSameReduceKeyHashSeparator;
        }

        public class OutputReduceToCollectionReferencesCommand
        {
            private readonly MapReduceIndex _index;
            private readonly string _outputReduceToCollection;
            private readonly string _referencesCollectionName;
            private readonly DocumentDatabase _database;
            private readonly Dictionary<string, HashSet<string>> _referencesOfReduceOutputs = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            private readonly List<string> _deletedReduceOutputs = new List<string>();
            private readonly Dictionary<string, HashSet<string>> _idsToDeleteByReferenceDocumentId = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            public OutputReduceToCollectionReferencesCommand(MapReduceIndex index, string outputReduceToCollection, string referencesCollectionName)
            {
                _index = index;
                _outputReduceToCollection = outputReduceToCollection;
                _referencesCollectionName = referencesCollectionName;
                _database = index.DocumentDatabase;
            }

            public void Add(string referenceDocumentId, string reduceOutputDocumentId)
            {
                if (_referencesOfReduceOutputs.TryGetValue(referenceDocumentId, out var values) == false)
                {
                    values = new HashSet<string>(1, StringComparer.OrdinalIgnoreCase);

                    _referencesOfReduceOutputs.Add(referenceDocumentId, values);
                }

                values.Add(reduceOutputDocumentId);
            }

            public void Delete(string reduceOutputId)
            {
                _deletedReduceOutputs.Add(reduceOutputId);
            }

            public void Execute(DocumentsOperationContext context, Transaction indexWriteTransaction)
            {
                // delete

                foreach (string reduceOutputId in _deletedReduceOutputs)
                {
                    var referenceId = _index.OutputReduceToCollection.GetPatternGeneratedIdForReduceOutput(indexWriteTransaction, reduceOutputId);

                    if (referenceId == null)
                        continue;

                    if (_idsToDeleteByReferenceDocumentId.TryGetValue(referenceId, out var values) == false)
                    {
                        values = new HashSet<string>(1, StringComparer.OrdinalIgnoreCase);

                        _idsToDeleteByReferenceDocumentId.Add(referenceId, values);
                    }

                    values.Add(reduceOutputId);
                }

                foreach (var reduceReferenceIdToReduceOutputIds in _idsToDeleteByReferenceDocumentId)
                {
                    using (var referenceDocument = _database.DocumentsStorage.Get(context, reduceReferenceIdToReduceOutputIds.Key))
                    {
                        if (referenceDocument == null)
                            continue;

                        if (referenceDocument.Data.TryGet(nameof(OutputReduceToCollectionReference.ReduceOutputs), out BlittableJsonReaderArray ids) == false)
                            ThrowReduceOutputsPropertyNotFound(referenceDocument.Id);

                        var idsToRemove = reduceReferenceIdToReduceOutputIds.Value;

                        if (idsToRemove.Count >= ids.Length)
                        {
                            Debug.Assert(ids.All(x => idsToRemove.Contains(x.ToString())), $"Found ID in {nameof(idsToRemove)} that aren't in {nameof(ids)}");

                            foreach (object deletedId in ids)
                            {
                                _index.OutputReduceToCollection.DeletePatternGeneratedIdForReduceOutput(indexWriteTransaction, deletedId.ToString());
                            }

                            _database.DocumentsStorage.Delete(context, referenceDocument.Id, null);
                            continue;
                        }

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
                            _database.DocumentsStorage.Put(context, referenceDocument.Id, null, doc);

                            foreach (var idToRemove in idsToRemove)
                            {
                                _index.OutputReduceToCollection.DeletePatternGeneratedIdForReduceOutput(indexWriteTransaction, idToRemove);
                            }
                        }
                    }
                }

                // put

                foreach (var referencesOfReduceOutput in _referencesOfReduceOutputs)
                {
                    using (var existingReferenceDocument = _database.DocumentsStorage.Get(context, referencesOfReduceOutput.Key))
                    {
                        var uniqueIds = referencesOfReduceOutput.Value;

                        if (existingReferenceDocument != null)
                        {
                            if (existingReferenceDocument.Data.TryGet(nameof(OutputReduceToCollectionReference.ReduceOutputs), out BlittableJsonReaderArray existingIds) == false)
                                ThrowReduceOutputsPropertyNotFound(existingReferenceDocument.Id);

                            foreach (object id in existingIds)
                            {
                                uniqueIds.Add(id.ToString());
                            }
                        }

                        var referenceDoc = new DynamicJsonValue
                        {
                            [nameof(OutputReduceToCollectionReference.ReduceOutputs)] = new DynamicJsonArray(uniqueIds),
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = _referencesCollectionName ?? $"{_outputReduceToCollection}/References"
                            }
                        };

                        using (var referenceJson = context.ReadObject(referenceDoc, "reference-of-reduce-output", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        {
                            _database.DocumentsStorage.Delete(context, referencesOfReduceOutput.Key, null);

                            _database.DocumentsStorage.Put(context, referencesOfReduceOutput.Key, null, referenceJson,
                                flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);

                            foreach (var reduceOutputId in referencesOfReduceOutput.Value)
                            {
                                _index.OutputReduceToCollection.AddPatternGeneratedIdForReduceOutput(indexWriteTransaction, reduceOutputId,
                                    referencesOfReduceOutput.Key);
                            }
                        }
                    }
                }
            }

            private static void ThrowReduceOutputsPropertyNotFound(string id)
            {
                throw new InvalidOperationException($"Property {nameof(OutputReduceToCollectionReference.ReduceOutputs)} was not found in document: {id}");
            }
        }
    }

    public class OutputReduceToCollectionCommandDto : TransactionOperationsMerger.IReplayableCommandDto<OutputReduceToCollectionCommand>
    {
        public string OutputReduceToCollection;
        public long? ReduceOutputIndex;
        public Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>> ReduceDocuments;

        public OutputReduceToCollectionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new OutputReduceToCollectionCommand(database, OutputReduceToCollection, ReduceOutputIndex, ReduceDocuments);
            return command;
        }
    }
}
