using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class OutputReduceIndexWriteOperation : IndexWriteOperation
    {
        private readonly OutputReduceToCollectionCommand _outputReduceToCollectionCommand;

        public OutputReduceIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction,
            LuceneIndexPersistence persistence, JsonOperationContext indexContext)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            var outputReduceToCollection = index.Definition.OutputReduceToCollection;
            var reduceOutputVersion = index.Definition.ReduceOutputIndex;

            Debug.Assert(string.IsNullOrWhiteSpace(outputReduceToCollection) == false);
            _outputReduceToCollectionCommand = new OutputReduceToCollectionCommand(DocumentDatabase, outputReduceToCollection, reduceOutputVersion, index, indexContext);
        }

        public override void Commit(IndexingStatsScope stats)
        {
            var enqueue = DocumentDatabase.TxMerger.Enqueue(_outputReduceToCollectionCommand);
            base.Commit(stats);
            try
            {
                using (stats.For(IndexingOperation.Reduce.SaveOutputDocuments))
                {
                    enqueue.GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                throw new IndexWriteException("Failed to save output reduce documents to disk", e);
            }
        }

        public override void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            base.IndexDocument(key, document, stats, indexContext);

            _outputReduceToCollectionCommand.AddReduce(key, document);
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            throw new NotSupportedException("Deleting index entries by id() field isn't supported by map-reduce indexes");
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            base.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommand.DeleteReduce(reduceKeyHash);
        }

        public override void Dispose()
        {
            base.Dispose();

            _outputReduceToCollectionCommand.Dispose();
        }

        public class OutputReduceToCollectionCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private const string MultipleOutputsForSameReduceKeyHashSeparator = "/";

            private readonly DocumentDatabase _database;
            private readonly string _outputReduceToCollection;
            private readonly long? _reduceOutputIndex;
            private readonly MapReduceIndex _index;
            private readonly List<string> _reduceKeyHashesToDelete = new List<string>();

            private readonly Dictionary<string, List<object>> _reduceDocuments = new Dictionary<string, List<object>>();
            private readonly Dictionary<string, List<BlittableJsonReaderObject>> _reduceDocumentsForReplayTransaction;
            private readonly JsonOperationContext _indexContext;

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex, MapReduceIndex index, JsonOperationContext context)
                : this(database, outputReduceToCollection, reduceOutputIndex)
            {
                _index = index;
                _indexContext = context;
            }

            /// <summary>
            ///This constructor should be used for replay transaction operations only
            /// </summary>
            internal OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex, Dictionary<string, List<BlittableJsonReaderObject>> reduceDocuments)
                : this(database, outputReduceToCollection, reduceOutputIndex)
            {
                _reduceDocumentsForReplayTransaction = reduceDocuments;
            }

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, long? reduceOutputIndex)
            {
                _database = database;
                _outputReduceToCollection = outputReduceToCollection;
                _reduceOutputIndex = reduceOutputIndex;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                foreach (var reduceKeyHash in _reduceKeyHashesToDelete)
                {
                    var deleteResult = _database.DocumentsStorage.Delete(context, GetOutputDocumentKey(reduceKeyHash), null);

                    if (deleteResult == null)
                    {
                        // document with a given reduce key hash doesn't exist 
                        // let's try to delete documents by reduce key hash prefix in case we got hash collision

                        _database.DocumentsStorage.DeleteDocumentsStartingWith(context, GetOutputDocumentKeyForSameReduceKeyHashPrefix(reduceKeyHash));
                    }
                }

                if (_reduceDocumentsForReplayTransaction != null)
                {
                    ProcessReduceDocumentsForReplayTransaction(context);
                    return _reduceDocumentsForReplayTransaction.Count;
                }

                foreach (var output in _reduceDocuments)
                {
                    for (var i = 0; i < output.Value.Count; i++) // we have hash collision so there might be multiple outputs for the same reduce key hash
                    {
                        var key = output.Value.Count == 1 ? GetOutputDocumentKey(output.Key) : GetOutputDocumentKeyForSameReduceKeyHash(output.Key, i);
                        var obj = output.Value[i];

                        using (var doc = GenerateReduceOutput(obj))
                        {
                            _database.DocumentsStorage.Put(context, key, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(key, doc.Size);
                        }
                    }
                }

                return _reduceDocuments.Count;
            }

            private BlittableJsonReaderObject GenerateReduceOutput(object obj)
            {
                var djv = new DynamicJsonValue();

                foreach (var property in _index.OutputReduceToCollectionPropertyAccessor.GetPropertiesInOrder(obj))
                {
                    var value = property.Value;
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _indexContext);
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
                        var doc = output.Value[i];

                        using (doc)
                        {
                            _database.DocumentsStorage.Put(context, key, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(key, doc.Size);
                        }
                    }
                }
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new OutputReduceToCollectionCommandDto
                {
                    OutputReduceToCollection = _outputReduceToCollection,
                    ReduceOutputIndex = _reduceOutputIndex,
                    ReduceDocuments = _reduceDocuments.ToDictionary(x => x.Key, y => y.Value.Select(GenerateReduceOutput).ToList())
            };
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
        }
    }

    public class OutputReduceToCollectionCommandDto : TransactionOperationsMerger.IReplayableCommandDto<OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand>
    {
        public string OutputReduceToCollection;
        public long? ReduceOutputIndex;
        public Dictionary<string, List<BlittableJsonReaderObject>> ReduceDocuments;

        public OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand(database, OutputReduceToCollection, ReduceOutputIndex, ReduceDocuments);
            return command;
        }
    }
}
