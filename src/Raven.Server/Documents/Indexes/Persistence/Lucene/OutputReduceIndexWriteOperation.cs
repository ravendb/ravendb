using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Indexing;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
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

        protected override void DisposeInternal()
        {
            using (Stats.SaveOutputDocumentsStats.Start())
            {
                var enqueue = DocumentDatabase.TxMerger.Enqueue(_outputReduceToCollectionCommand);
                _writer?.Commit(); // just make sure changes are flushed to disk
                try
                {
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
        }

        public override void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            base.IndexDocument(key, document, stats, indexContext);

            _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
            {
                ReduceKeyHash = key,
                Document = document
            });
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            base.Delete(key, stats);



            _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
            {
                IsDelete = true,
                ReduceKeyHash = key,
            });
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            base.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
            {
                IsDelete = true,
                ReduceKeyHash = reduceKeyHash,
            });
        }

        public class OutputReduceToCollectionCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly string _outputReduceToCollection;
            private readonly DocumentConvention _documentConvention = new DocumentConvention();
            private readonly EntityToBlittable _entityToBlittable = new EntityToBlittable(null);
            public readonly List<OutputReduceDocument> ReduceDocuments = new List<OutputReduceDocument>();

            public OutputReduceToCollectionCommand(DocumentDatabase database, string outputReduceToCollection)
            {
                _database = database;
                _outputReduceToCollection = outputReduceToCollection;
            }

            public class OutputReduceDocument
            {
                public bool IsDelete;
                public LazyStringValue ReduceKeyHash;
                public object Document;
            }

            public override void Execute(DocumentsOperationContext context)
            {
                foreach (var reduceDocument in ReduceDocuments)
                {
                    var id = _outputReduceToCollection + "/" + reduceDocument.ReduceKeyHash;

                    if (reduceDocument.IsDelete)
                    {
                        _database.DocumentsStorage.Delete(context, id, null);
                        continue;
                    }

                    var documentInfo = new DocumentInfo
                    {
                        Id = id,
                        Collection = _outputReduceToCollection
                    };
                    using (var document = _entityToBlittable.ConvertEntityToBlittable(reduceDocument.Document, _documentConvention, context, documentInfo))
                    {
                        _database.DocumentsStorage.Put(context, id, null, document, flags: DocumentFlags.Artificial | DocumentFlags.FromIndex);
                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(id, document.Size);
                    }
                }
            }
        }
    }
}