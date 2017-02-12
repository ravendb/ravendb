using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Indexing;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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
            _outputReduceToCollectionCommand = new OutputReduceToCollectionCommand(DocumentDatabase, outputReduceToCollection);
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
                DocumentDatabase.NotificationCenter.Add(AlertRaised.Create(
                    "Save Reduce Index Output",
                    "Failed to save output documnts of reduce index to disk",
                    AlertType.ErrorSavingReduceOutputDocuments,
                    NotificationSeverity.Error,
                    key: _indexName,
                    details: new ExceptionDetails(e)));
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
            private readonly List<OutputReduceDocument> _reduceDocuments = new List<OutputReduceDocument>();
            private readonly JsonOperationContext _jsonContext;
            private PropertyAccessor _propertyAccessor;

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection)
            {
                _database = database;
                _outputReduceToCollection = outputReduceToCollection;
                _jsonContext = JsonOperationContext.ShortTermSingleUse();
            }

            private class OutputReduceDocument
            {
                public bool IsDelete;
                public string Key;
                public BlittableJsonReaderObject Document;
            }

            public override void Execute(DocumentsOperationContext context)
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
            }

            public void AddReduce(string reduceKeyHash, object reduceObject)
            {
                var key = _outputReduceToCollection + "/" + reduceKeyHash;

                var djv = new DynamicJsonValue();

                if (_propertyAccessor == null)
                    _propertyAccessor = PropertyAccessor.Create(reduceObject.GetType());
                foreach (var property in _propertyAccessor.PropertiesInOrder)
                {
                    var value = property.Value.GetValue(reduceObject);
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value);
                }
                djv[Constants.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Metadata.Collection] = _outputReduceToCollection
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