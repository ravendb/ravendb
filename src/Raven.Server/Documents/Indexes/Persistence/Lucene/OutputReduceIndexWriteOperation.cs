using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            Debug.Assert(string.IsNullOrWhiteSpace(outputReduceToCollection) == false);
            _outputReduceToCollectionCommand = new OutputReduceToCollectionCommand(DocumentDatabase, outputReduceToCollection, index, indexContext);
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
            private readonly MapReduceIndex _index;
            private readonly List<string> _reduceKeyHashesToDelete = new List<string>();

            private readonly Dictionary<string, List<BlittableJsonReaderObject>> _reduceDocuments =
                new Dictionary<string, List<BlittableJsonReaderObject>>();
            private readonly JsonOperationContext _indexContext;

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, MapReduceIndex index, JsonOperationContext context)
                : this(database, outputReduceToCollection)
            {
                _index = index;
                _indexContext = context;
            }

            /// <summary>
            ///This constructor should be used for replay transaction operations only
            /// </summary>
            internal OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, Dictionary<string, List<BlittableJsonReaderObject>> reduceDocuments)
                : this(database, outputReduceToCollection)
            {
                _reduceDocuments = reduceDocuments;
            }

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection)
            {
                _database = database;
                _outputReduceToCollection = outputReduceToCollection;
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

                foreach (var output in _reduceDocuments)
                {
                    for (var i = 0; i < output.Value.Count; i++) // we have hash collision so there might be multiple outputs for the same reduce key hash
                    {
                        var key = output.Value.Count == 1 ? GetOutputDocumentKey(output.Key) : GetOutputDocumentKeyForSameReduceKeyHash(output.Key, i);
                        var doc = output.Value[i];

                        _database.DocumentsStorage.Put(context, key, null, doc, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);

                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(key, doc.Size);
                    }
                }

                return _reduceDocuments.Count;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new OutputReduceToCollectionCommandDto
                {
                    OutputReduceToCollection = _outputReduceToCollection,
                    ReduceDocuments = _reduceDocuments
                };
            }

            public void AddReduce(string reduceKeyHash, object reduceObject)
            {
                if (_reduceDocuments.TryGetValue(reduceKeyHash, out var outputs) == false)
                {
                    outputs = new List<BlittableJsonReaderObject>(1);
                    _reduceDocuments.Add(reduceKeyHash, outputs);
                }
                
                var djv = new DynamicJsonValue();

                if (_index.OutputReduceToCollectionPropertyAccessor == null)
                    _index.OutputReduceToCollectionPropertyAccessor = PropertyAccessor.Create(reduceObject.GetType(), reduceObject);
                foreach (var property in _index.OutputReduceToCollectionPropertyAccessor.GetPropertiesInOrder(reduceObject))
                {
                    var value = property.Value;
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _indexContext);
                }
                djv[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = _outputReduceToCollection
                };

                var doc = _indexContext.ReadObject(djv, "output-of-reduce-doc", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                outputs.Add(doc);
            }

            public void DeleteReduce(string reduceKeyHash)
            {
                _reduceKeyHashesToDelete.Add(reduceKeyHash);

                if (_reduceDocuments.Remove(reduceKeyHash, out var outputs))
                {
                    foreach (var bjro in outputs)
                    {
                        bjro.Dispose();
                    }
                }
            }

            private string GetOutputDocumentKey(string reduceKeyHash)
            {
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
                        doc.Dispose();
                    }
                }
            }
        }
    }

    public class OutputReduceToCollectionCommandDto : TransactionOperationsMerger.IReplayableCommandDto<OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand>
    {
        public string OutputReduceToCollection;
        public Dictionary<string, List<BlittableJsonReaderObject>> ReduceDocuments;

        public OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand(database, OutputReduceToCollection, ReduceDocuments);
            return command;
        }
    }
}
