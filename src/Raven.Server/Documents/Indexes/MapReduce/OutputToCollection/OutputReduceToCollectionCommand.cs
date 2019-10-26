using System;
using System.Collections.Generic;
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

        private readonly Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>> _reduceDocuments =
            new Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>>();

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
            Dictionary<string, List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>> reduceDocuments)
            : this(database, outputReduceToCollection, reduceOutputIndex)
        {
            _reduceDocuments = reduceDocuments;
        }

        private OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex)
        {
            _database = database;
            _outputReduceToCollection = outputReduceToCollection;
            _reduceOutputIndex = reduceOutputIndex;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
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
                            _outputReferences.DeleteFromReferenceDocument(doc.Id);
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
                    var doc = output.Value[i].Json;
                    var referenceDocumentId = output.Value[i].ReferenceDocumentId;

                    _database.DocumentsStorage.Put(context, id, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);

                    context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(id, doc.Size);

                    _outputReferences?.Add(referenceDocumentId, id);
                }
            }

            _outputReferences?.Execute(context);

            return _reduceDocuments.Count;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new OutputReduceToCollectionCommandDto
            {
                OutputReduceToCollection = _outputReduceToCollection, ReduceOutputIndex = _reduceOutputIndex, ReduceDocuments = _reduceDocuments
            };
        }

        public void AddReduce(string reduceKeyHash, object reduceObject)
        {
            if (_reduceDocuments.TryGetValue(reduceKeyHash, out var outputs) == false)
            {
                outputs = new List<(BlittableJsonReaderObject Json, string ReferenceDocumentId)>(1);
                _reduceDocuments.Add(reduceKeyHash, outputs);
            }

            var djv = new DynamicJsonValue();

            if (_index.OutputReduceToCollectionPropertyAccessor == null)
                _index.OutputReduceToCollectionPropertyAccessor = PropertyAccessor.Create(reduceObject.GetType(), reduceObject);

            OutputReferencesPattern.DocumentIdBuilder referenceDocIdBuilder = null;

            string referenceDocumentId = null;

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
            }

            djv[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = _outputReduceToCollection
            };

            var doc = _indexContext.ReadObject(djv, "output-of-reduce-doc", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            outputs.Add((doc, referenceDocumentId));
        }

        public void DeleteReduce(string reduceKeyHash)
        {
            _reduceKeyHashesToDelete.Add(reduceKeyHash);

            if (_reduceDocuments.Remove(reduceKeyHash, out var outputs))
            {
                foreach (var item in outputs)
                {
                    item.Json.Dispose();
                }
            }
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
            foreach (var r in _reduceDocuments)
            {
                foreach (var doc in r.Value)
                {
                    doc.Json.Dispose();
                }
            }
        }

        private class ReduceOutputReferencesSubCommand
        {
            private readonly MapReduceIndex _index;
            private readonly string _outputReduceToCollection;
            private readonly Transaction _indexWriteTransaction;
            private readonly DocumentDatabase _database;
            readonly Dictionary<string, HashSet<string>> _referencesOfReduceOutputs = new Dictionary<string, HashSet<string>>();
            readonly Dictionary<string, HashSet<string>> _idsToDeleteByReferenceDocumentId = new Dictionary<string, HashSet<string>>();

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
                    values = new HashSet<string>(1);

                    _referencesOfReduceOutputs.Add(referenceDocumentId, values);
                }

                values.Add(reduceOutputDocumentId);
            }

            public void DeleteFromReferenceDocument(string reduceOutputId)
            {
                var referenceId = _index.ReduceOutputs.GetPatternGeneratedIdForReduceOutput(_indexWriteTransaction, reduceOutputId);

                if (_idsToDeleteByReferenceDocumentId.TryGetValue(referenceId, out var values) == false)
                {
                    values = new HashSet<string>(1);

                    _idsToDeleteByReferenceDocumentId.Add(referenceId, values);
                }

                values.Add(reduceOutputId);
            }

            public void Execute(DocumentsOperationContext context)
            {
                // delete

                foreach (var reduceReferenceIdToReduceOutputIds in _idsToDeleteByReferenceDocumentId)
                {
                    var referenceDocument = _database.DocumentsStorage.Get(context, reduceReferenceIdToReduceOutputIds.Key);
                    
                    if (referenceDocument == null)
                        continue;

                    if (referenceDocument.Data.TryGet(nameof(ReduceOutputIdsReference.ReduceOutputs), out BlittableJsonReaderArray ids) == false)
                        ThrowIdsPropertyNotFound(referenceDocument.Id);

                    var idsToRemove = reduceReferenceIdToReduceOutputIds.Value;

                    if (idsToRemove.Count >= ids.Length)
                    {
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

                    var doc = context.ReadObject(referenceDocument.Data, referenceDocument.Id);

                    _database.DocumentsStorage.Put(context, referenceDocument.Id, null, doc);

                    _index.ReduceOutputs.DeletePatternGeneratedIdForReduceOutput(_indexWriteTransaction, referenceDocument.Id);
                }

                // put

                foreach (var referencesOfReduceOutput in _referencesOfReduceOutputs)
                {
                    var referenceDoc = new DynamicJsonValue
                    {
                        [nameof(ReduceOutputIdsReference.ReduceOutputs)] = new DynamicJsonArray(referencesOfReduceOutput.Value),
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = $"{_outputReduceToCollection}/References"
                        }
                    };

                    var referenceJson = context.ReadObject(referenceDoc, "reference-of-reduce-output", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    _database.DocumentsStorage.Put(context, referencesOfReduceOutput.Key, null, referenceJson, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);

                    foreach (var reduceOutputId in referencesOfReduceOutput.Value)
                    {
                        _index.ReduceOutputs.AddPatternGeneratedIdForReduceOutput(_indexWriteTransaction, reduceOutputId, referencesOfReduceOutput.Key);
                    }
                }
            }

            private static void ThrowIdsPropertyNotFound(string id)
            {
                throw new InvalidOperationException($"Property {nameof(ReduceOutputIdsReference.ReduceOutputs)} was not found in document: {id}");
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
