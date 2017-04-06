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

        public OutputReduceIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            var outputReduceToCollection = index.Definition.OutputReduceToCollection;
            Debug.Assert(string.IsNullOrWhiteSpace(outputReduceToCollection) == false);
            _outputReduceToCollectionCommand = new OutputReduceToCollectionCommand(DocumentDatabase, outputReduceToCollection, index);
        }

        public override void Commit(IndexingStatsScope stats)
        {
            var enqueue = DocumentDatabase.TxMerger.Enqueue(_outputReduceToCollectionCommand);
            base.Commit(stats);
            try
            {
                using (stats.For(IndexingOperation.Reduce.SaveOutputDocuments))
                    enqueue.Wait();
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
            base.Delete(key, stats);

            _outputReduceToCollectionCommand.DeleteReduce(key);
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            base.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommand.DeleteReduce(reduceKeyHash);
        }

        public class OutputReduceToCollectionCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly string _outputReduceToCollection;
            private readonly MapReduceIndex _index;
            private readonly List<OutputReduceDocument> _reduceDocuments = new List<OutputReduceDocument>();
            private readonly JsonOperationContext _jsonContext;

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection, MapReduceIndex index)
            {
                _database = database;
                _outputReduceToCollection = outputReduceToCollection;
                _index = index;
                _jsonContext = JsonOperationContext.ShortTermSingleUse();
            }

            private class OutputReduceDocument
            {
                public bool IsDelete;
                public string Key;
                public BlittableJsonReaderObject Document;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var reduceDocument in _reduceDocuments)
                {
                    var key = reduceDocument.Key;
                    if (reduceDocument.IsDelete)
                    {
                        _database.DocumentsStorage.Delete(context, key, null);
                        continue;
                    }

                    using (var document = reduceDocument.Document)
                    {
                        _database.DocumentsStorage.Put(context, key, null, document, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(key, document.Size);
                    }
                }
                return _reduceDocuments.Count;
            }

            public void AddReduce(string reduceKeyHash, object reduceObject)
            {
                var key = _outputReduceToCollection + "/" + reduceKeyHash;

                var djv = new DynamicJsonValue();

                if (_index.OutputReduceToCollectionPropertyAccessor == null)
                    _index.OutputReduceToCollectionPropertyAccessor = PropertyAccessor.Create(reduceObject.GetType());
                foreach (var property in _index.OutputReduceToCollectionPropertyAccessor.PropertiesInOrder)
                {
                    var value = property.Value.GetValue(reduceObject);
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value);
                }
                djv[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = _outputReduceToCollection
                };

                _reduceDocuments.Add(new OutputReduceDocument
                {
                    Key = key,
                    Document = _jsonContext.ReadObject(djv, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk),
                });
            }

            public void DeleteReduce(string reduceKeyHash)
            {
                var key = _outputReduceToCollection + "/" + reduceKeyHash;

                _reduceDocuments.Add(new OutputReduceDocument
                {
                    IsDelete = true,
                    Key = key,
                });
            }

            public void Dispose()
            {
                _jsonContext?.Dispose();
            }
        }
    }
}