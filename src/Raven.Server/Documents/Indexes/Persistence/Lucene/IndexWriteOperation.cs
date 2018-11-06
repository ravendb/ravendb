using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly Term _documentId = new Term(Constants.Documents.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, "Dummy");

        protected readonly LuceneIndexWriter _writer;
        protected readonly Dictionary<string, LuceneSuggestionIndexWriter> _suggestionsWriters;
        private readonly bool _hasSuggestions;

        private readonly LuceneDocumentConverterBase _converter;
        protected readonly DocumentDatabase DocumentDatabase;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        private IndexingStatsScope _statsInstance;
        protected readonly IndexWriteOperationStats Stats = new IndexWriteOperationStats();

        private readonly IState _state;
        private readonly LuceneVoronDirectory _directory;

        public IndexWriteOperation(Index index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index, LoggingSource.Instance.GetLogger<IndexWriteOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            _converter = converter;
            DocumentDatabase = index._indexStorage.DocumentDatabase;

            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), index.Definition);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            try
            {                
                _releaseWriteTransaction = directory.SetTransaction(writeTransaction, out _state);
                _writer = persistence.EnsureIndexWriter(_state);

                _suggestionsWriters = persistence.EnsureSuggestionIndexWriter(_state);
                _hasSuggestions = _suggestionsWriters.Count > 0;
                
                _locker = directory.MakeLock("writing-to-index.lock");
                _directory = directory;

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
            if (_writer != null)
            {
                using (stats.For(IndexingOperation.Lucene.FlushToDisk))
                {
                    _writer.Commit(_state); // just make sure changes are flushed to disk

                    if (_hasSuggestions)
                    {
                        foreach (var item in _suggestionsWriters)
                            item.Value.Commit(_state);
                    }
                }                    
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
                    _writer.AddDocument(_converter.Document, _analyzer, _state);

                if (_hasSuggestions)
                {
                    using (Stats.SuggestionStats.Start())
                    {
                        foreach (var item in _suggestionsWriters)
                        {
                            var writer = item.Value;
                            writer.AddDocument(_converter.Document, _analyzer, _state);
                        }
                    }
                }

                stats.RecordIndexingOutput();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Indexed document for '{_indexName}'. Key: {key}.");
            }
        }

        public (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
        {
            var usedMemory = _writer.RamSizeInBytes();
            var fileAllocations = _directory.GetFilesAllocations();

            if (_hasSuggestions)
            {
                foreach (var item in _suggestionsWriters)
                {
                    usedMemory += item.Value.RamSizeInBytes();
                    fileAllocations += item.Value.FilesAllocationsInBytes();
                }
            }

            return (usedMemory, fileAllocations);
        }

        public virtual void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_documentId.CreateTerm(key), _state);
        }

        public virtual void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash), _state);

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
            Stats.SuggestionStats = stats.For(IndexingOperation.Lucene.Suggestion, start: false);
        }

        protected class IndexWriteOperationStats
        {
            public IndexingStatsScope DeleteStats;
            public IndexingStatsScope ConvertStats;
            public IndexingStatsScope AddStats;
            public IndexingStatsScope SuggestionStats;
        }
    }
}
