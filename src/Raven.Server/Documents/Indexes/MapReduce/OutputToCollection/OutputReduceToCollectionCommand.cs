using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Impl;
using Object = System.Object;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceToCollectionCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
    {
        private const string MultipleOutputsForSameReduceKeyHashSeparator = "/";

        private readonly DocumentDatabase _database;
        private readonly string _outputReduceToCollection;
        private readonly long? _reduceOutputIndex;
        private readonly OutputReferencesPattern _patternForReduceOutputReferences;
        private readonly MapReduceIndex _index;
        private readonly List<string> _reduceKeyHashesToDelete = new List<string>();

        private readonly Dictionary<string, List<object>> _reduceDocuments =
            new Dictionary<string, List<object>>();

        private readonly Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>> _reduceDocumentsForReplayTransaction =
            new Dictionary<string, List<(BlittableJsonReaderObject, string)>>();

        private readonly JsonOperationContext _indexContext;
        private readonly ReduceOutputReferencesSubCommand _outputReferences;

        public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex,
            OutputReferencesPattern patternForReduceOutputReferences, MapReduceIndex index, JsonOperationContext context, Transaction indexWriteTransaction)
            : this(database, outputReduceToCollection, reduceOutputIndex)
        {
            _patternForReduceOutputReferences = patternForReduceOutputReferences;
            _index = index;
            _indexContext = context;

            if (_patternForReduceOutputReferences != null)
                _outputReferences = new ReduceOutputReferencesSubCommand(index, outputReduceToCollection, indexWriteTransaction);
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

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            if (_reduceDocumentsForReplayTransaction != null && _reduceDocumentsForReplayTransaction.Count > 0)
            {
                ProcessReduceDocumentsForReplayTransaction(context);
                return _reduceDocumentsForReplayTransaction.Count;
            }

            foreach (var reduceKeyHash in _reduceKeyHashesToDelete)
            {
                var id = GetOutputDocumentKey(reduceKeyHash);

                var deleteResult = _database.DocumentsStorage.Delete(context, id, null);

                if (deleteResult != null)
                {
                    _outputReferences?.DeleteFromReferenceDocument(id);
                }
                else
                {
                    // document with a given reduce key hash doesn't exist 
                    // let's try to delete documents by reduce key hash prefix in case we got hash collision

                    var prefix = GetOutputDocumentKeyForSameReduceKeyHashPrefix(reduceKeyHash);

                    if (_outputReferences != null)
                    {
                        var docs = _database.DocumentsStorage.GetDocumentsStartingWith(context, prefix, null, null, null, 0, int.MaxValue);

                        foreach (var doc in docs)
                        {
                            using (doc)
                            {
                                _outputReferences.DeleteFromReferenceDocument(doc.Id);
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
                    var obj = output.Value[i];
                    string referenceDocumentId;

                    using (var doc = GenerateReduceOutput(obj, out referenceDocumentId))
                    {
                        _database.DocumentsStorage.Put(context, id, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(id, doc.Size);
                    }
                    
                    if (referenceDocumentId != null)
                        _outputReferences?.Add(referenceDocumentId, id);
                }
            }

            _outputReferences?.Execute(context);

            return _reduceDocuments.Count;
        }

        private BlittableJsonReaderObject GenerateReduceOutput(object reduceObject, out string referenceDocumentId)
        {
            var djv = new DynamicJsonValue();

            OutputReferencesPattern.DocumentIdBuilder referenceDocIdBuilder = null;

            using (_patternForReduceOutputReferences?.BuildReferenceDocumentId(out referenceDocIdBuilder))
            {
                foreach (var property in _index.OutputReduceToCollectionPropertyAccessor.GetPropertiesInOrder(reduceObject))
                {
                    var value = property.Value;
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _indexContext);

                    if (referenceDocIdBuilder?.ContainsField(property.Key) == true)
                        referenceDocIdBuilder.Add(property.Key, property.Value);
                }

                if (referenceDocIdBuilder != null)
                    referenceDocumentId = referenceDocIdBuilder.GetId();
                else
                    referenceDocumentId = null;
            }

            djv[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = _outputReduceToCollection
            };

            return _indexContext.ReadObject(djv, "output-of-reduce-doc", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
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

            dto.ReduceDocuments = _reduceDocuments.ToDictionary(x => x.Key, y => y.Value.Select(i =>
            {
                var doc = GenerateReduceOutput(i, out var referenceDocumentId);

                return (doc, referenceDocumentId);
            }).ToList());

            return dto;
        }

        public void AddReduce(string reduceKeyHash, object reduceObject)
        {
            if (_reduceDocuments.TryGetValue(reduceKeyHash, out var outputs) == false)
            {
                outputs = new List<object>(1);
                _reduceDocuments.Add(reduceKeyHash, outputs);
            }

            if (_index.OutputReduceToCollectionPropertyAccessor == null)
                _index.OutputReduceToCollectionPropertyAccessor = PropertyAccessor.Create(reduceObject.GetType(), reduceObject);

            outputs.Add(reduceObject);
        }

        public void DeleteReduce(string reduceKeyHash)
        {
            _reduceKeyHashesToDelete.Add(reduceKeyHash);

            _reduceDocuments.Remove(reduceKeyHash, out _);
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

        public void Dispose()
        {
        }

        private class ReduceOutputReferencesSubCommand
        {
            private readonly MapReduceIndex _index;
            private readonly string _outputReduceToCollection;
            private readonly Transaction _indexWriteTransaction;
            private readonly DocumentDatabase _database;
            readonly Dictionary<string, HashSet<string>> _referencesOfReduceOutputs = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            readonly Dictionary<string, HashSet<string>> _idsToDeleteByReferenceDocumentId = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            public ReduceOutputReferencesSubCommand(MapReduceIndex index, string outputReduceToCollection, Transaction indexWriteTransaction)
            {
                _index = index;
                _outputReduceToCollection = outputReduceToCollection;
                _indexWriteTransaction = indexWriteTransaction;
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

            public void DeleteFromReferenceDocument(string reduceOutputId)
            {
                var referenceId = _index.ReduceOutputs.GetPatternGeneratedIdForReduceOutput(_indexWriteTransaction, reduceOutputId);

                if (_idsToDeleteByReferenceDocumentId.TryGetValue(referenceId, out var values) == false)
                {
                    values = new HashSet<string>(1, StringComparer.OrdinalIgnoreCase);

                    _idsToDeleteByReferenceDocumentId.Add(referenceId, values);
                }

                values.Add(reduceOutputId);
            }

            public void Execute(DocumentsOperationContext context)
            {
                // delete

                foreach (var reduceReferenceIdToReduceOutputIds in _idsToDeleteByReferenceDocumentId)
                {
                    using (var referenceDocument = _database.DocumentsStorage.Get(context, reduceReferenceIdToReduceOutputIds.Key))
                    {
                        if (referenceDocument == null)
                            continue;

                        if (referenceDocument.Data.TryGet(nameof(OutputReduceToCollectionReference.ReduceOutputs), out BlittableJsonReaderArray ids) == false)
                            ThrowIdsPropertyNotFound(referenceDocument.Id);

                        var idsToRemove = reduceReferenceIdToReduceOutputIds.Value;

                        if (idsToRemove.Count >= ids.Length)
                        {
                            Debug.Assert(ids.All(x => idsToRemove.Contains(x.ToString())), $"Found ID in {nameof(idsToRemove)} that aren't in {nameof(ids)}");

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

                            _index.ReduceOutputs.DeletePatternGeneratedIdForReduceOutput(_indexWriteTransaction, referenceDocument.Id);
                        }
                    }
                }

                // put

                foreach (var referencesOfReduceOutput in _referencesOfReduceOutputs)
                {
                    var referenceDoc = new DynamicJsonValue
                    {
                        [nameof(OutputReduceToCollectionReference.ReduceOutputs)] = new DynamicJsonArray(referencesOfReduceOutput.Value),
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = $"{_outputReduceToCollection}/References"
                        }
                    };

                    using (var referenceJson = context.ReadObject(referenceDoc, "reference-of-reduce-output", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {
                        _database.DocumentsStorage.Put(context, referencesOfReduceOutput.Key, null, referenceJson,
                            flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);

                        foreach (var reduceOutputId in referencesOfReduceOutput.Value)
                        {
                            _index.ReduceOutputs.AddPatternGeneratedIdForReduceOutput(_indexWriteTransaction, reduceOutputId, referencesOfReduceOutput.Key);
                        }
                    }
                }
            }

            private static void ThrowIdsPropertyNotFound(string id)
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
