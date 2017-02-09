using System;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.NewClient.Abstractions.Data;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

using Raven.NewClient.Data.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly Term _documentId = new Term(Constants.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Indexing.Fields.ReduceKeyFieldName, "Dummy");

        protected readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverterBase _converter;
        protected readonly DocumentDatabase DocumentDatabase;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        private IndexingStatsScope _statsInstance;
        protected readonly IndexWriteOperationStats Stats = new IndexWriteOperationStats();

        public IndexWriteOperation(Index index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index.Definition.Name, LoggingSource.Instance.GetLogger<IndexWriteOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            _converter = converter;
            DocumentDatabase = index._indexStorage.DocumentDatabase;

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
            }

        public override void Dispose()
        {
            try
            {
                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();
            }
        }

        public virtual void Commit(IndexingStatsScope stats)
        {
            if (_writer != null) // TODO && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
            {
                using (stats.For(IndexingOperation.Lucene.FlushToDisk))
                    _writer.Commit(); // just make sure changes are flushed to disk
            }
            }

        public virtual void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
            {
            EnsureValidStats(stats);

            bool shouldSkip;
            IDisposable setDocument;
            using (Stats.ConvertStats.Start())
                setDocument = _converter.SetDocument(key, document, indexContext, out shouldSkip);

            using (setDocument)
            {
                if (shouldSkip)
                    return;

                using (Stats.AddStats.Start())
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

        public virtual void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_documentId.CreateTerm(key));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleted document for '{_indexName}'. Key: {key}.");
        }

        public virtual void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
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

            Stats.DeleteStats = stats.For(IndexingOperation.Lucene.Delete, start: false);
            Stats.AddStats = stats.For(IndexingOperation.Lucene.AddDocument, start: false);
            Stats.ConvertStats = stats.For(IndexingOperation.Lucene.Convert, start: false);
        }

        protected class IndexWriteOperationStats
        {
            public IndexingStatsScope DeleteStats;
            public IndexingStatsScope ConvertStats;
            public IndexingStatsScope AddStats;
        }
    }
}