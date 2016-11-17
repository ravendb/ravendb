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

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly Term _documentId = new Term(Constants.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Indexing.Fields.ReduceKeyFieldName, "Dummy");

        private readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverterBase _converter;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        private IndexingStatsScope _statsInstance;
        private IndexWriteOperationStats _stats = new IndexWriteOperationStats();

        public IndexWriteOperation(string indexName, Dictionary<string, IndexField> fields,
            LuceneVoronDirectory directory, LuceneDocumentConverterBase converter,
            Transaction writeTransaction, LuceneIndexPersistence persistence, DocumentDatabase documentDatabase)
            : base(indexName, LoggingSource.Instance.GetLogger<IndexWriteOperation>(documentDatabase.Name))
        {
            _converter = converter;
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields);
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
        }

        public override void Dispose()
        {
            try
            {
                if (_writer != null) // TODO && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
                    _writer.Commit(); // just make sure changes are flushed to disk

                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();
            }
        }

        public void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);

            IDisposable setDocument;
            using (_stats.ConvertStats.Start())
                setDocument = _converter.SetDocument(key, document, indexContext);

            using (setDocument)
            {
                using (_stats.AddStats.Start())
                    _writer.AddDocument(_converter.Document, _analyzer);

                stats.RecordIndexingOutput();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Indexed document for '{_indexName}'. Key: {key}. Output: {_converter.Document}.");
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
        }

        public void DeleteReduceResult(string reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (_stats.DeleteStats.Start())
                _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleted document for '{_indexName}'. Reduce key hash: {reduceKeyHash}.");
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
        }

        private class IndexWriteOperationStats
        {
            public IndexingStatsScope DeleteStats;
            public IndexingStatsScope ConvertStats;
            public IndexingStatsScope AddStats;
        }
    }
}