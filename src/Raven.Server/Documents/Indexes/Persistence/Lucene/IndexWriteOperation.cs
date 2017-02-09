using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

using Constants = Raven.Abstractions.Data.Constants;
using Raven.Client.Data.Indexes;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly Term _documentId = new Term(Constants.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Indexing.Fields.ReduceKeyFieldName, "Dummy");

        private readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverterBase _converter;
        private readonly DocumentDatabase _documentDatabase;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        private IndexingStatsScope _statsInstance;
        private IndexWriteOperationStats _stats = new IndexWriteOperationStats();
        private readonly OutputReduceToCollectionCommand _outputReduceToCollectionCommand;

        public IndexWriteOperation(Index index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index.Definition.Name, LoggingSource.Instance.GetLogger<IndexWriteOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            _converter = converter;
            _documentDatabase = index._indexStorage.DocumentDatabase;

            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), index.Definition.MapFields);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            try
            {
                _releaseWriteTransaction = directory.SetTransaction(writeTransaction);

                _writer = persistence.EnsureIndexWriter();

                _locker = directory.MakeLock("writing-to-index.lock");

                if (_locker.Obtain() == false)
                    throw new InvalidOperationException($"Could not obtain the 'writing-to-index' lock for '{_indexName}' index.");
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }

            var mapReduceIndex = index as MapReduceIndex;
            var outputReduceToCollection = mapReduceIndex?.Definition.OutputReduceToCollection;
            if (string.IsNullOrWhiteSpace(outputReduceToCollection) == false)
            {
                _outputReduceToCollectionCommand = new OutputReduceToCollectionCommand(_documentDatabase, outputReduceToCollection);
            }
        }

        public override void Dispose()
        {
            try
            {
                if (_writer != null) // TODO && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
                {
                    if (_outputReduceToCollectionCommand == null)
                    {
                        _writer.Commit(); // just make sure changes are flushed to disk
                    }
                    else
                    {
                        using (_stats.SaveOutputDocumentsStats.Start())
                        {
                            var enqueue = _documentDatabase.TxMerger.Enqueue(_outputReduceToCollectionCommand);
                            _writer.Commit(); // just make sure changes are flushed to disk
                            try
                            {
                                enqueue.Wait();
                            }
                            catch (Exception e)
                            {
                                _documentDatabase.NotificationCenter.Add(AlertRaised.Create(
                                    "Save Reduce Index Output",
                                    "Failed to save output documnts of reduce index to disk",
                                    AlertType.ErrorSavingReduceOutputDocuments,
                                    NotificationSeverity.Error,
                                    key: _indexName,
                                    details: new ExceptionDetails(e)));
                            }
                        }
                    }
                }

                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();
            }
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
                public string ReduceKeyHash;
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

        public void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);

            bool shouldSkip;
            IDisposable setDocument;
            using (_stats.ConvertStats.Start())
                setDocument = _converter.SetDocument(key, document, indexContext, out shouldSkip);

            using (setDocument)
            {
                if (shouldSkip)
                    return;

                using (_stats.AddStats.Start())
                     _writer.AddDocument(_converter.Document, _analyzer);

                stats.RecordIndexingOutput();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Indexed document for '{_indexName}'. Key: {key}. Output: {_converter.Document}.");

                _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
                {
                    ReduceKeyHash = key,
                    Document = document
                });
            }
        }

        public long GetUsedMemory()
        {
            return _writer.RamSizeInBytes();
        }

        public void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (_stats.DeleteStats.Start())
                _writer.DeleteDocuments(_documentId.CreateTerm(key));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleted document for '{_indexName}'. Key: {key}.");

            _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
            {
                IsDelete = true,
                ReduceKeyHash = key,
            });
        }

        public void DeleteReduceResult(string reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (_stats.DeleteStats.Start())
                _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleted document for '{_indexName}'. Reduce key hash: {reduceKeyHash}.");

            _outputReduceToCollectionCommand?.ReduceDocuments.Add(new OutputReduceToCollectionCommand.OutputReduceDocument
            {
                IsDelete = true,
                ReduceKeyHash = reduceKeyHash,
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;

            _stats.DeleteStats = stats.For(IndexingOperation.Lucene.Delete, start: false);
            _stats.AddStats = stats.For(IndexingOperation.Lucene.AddDocument, start: false);
            _stats.ConvertStats = stats.For(IndexingOperation.Lucene.Convert, start: false);
            _stats.SaveOutputDocumentsStats = stats.For(IndexingOperation.Reduce.SaveOutputDocuments, start: false);
        }

        private class IndexWriteOperationStats
        {
            public IndexingStatsScope DeleteStats;
            public IndexingStatsScope ConvertStats;
            public IndexingStatsScope AddStats;
            public IndexingStatsScope SaveOutputDocumentsStats;
        }
    }
}