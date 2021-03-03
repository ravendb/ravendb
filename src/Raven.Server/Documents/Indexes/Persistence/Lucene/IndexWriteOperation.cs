using System;
using System.Buffers;
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
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase, IWriteOperationBuffer
    {
        private readonly Term _documentId = new Term(Constants.Documents.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, "Dummy");
        private readonly Term _sourceDocumentIdHash = new Term(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, "Dummy");

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

        private byte[] _buffer;

        public IndexWriteOperation(Index index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index, LoggingSource.Instance.GetLogger<IndexWriteOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            _converter = converter;
            DocumentDatabase = index._indexStorage.DocumentDatabase;

            try
            {
                _analyzer = CreateAnalyzer(index, index.Definition);
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
            catch (Exception e) when (e.IsOutOfMemory())
            {
                throw;
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
                _directory.ResetAllocations();
                if (_hasSuggestions)
                {
                    foreach (var suggestionIndexWriter in _suggestionsWriters)
                        suggestionIndexWriter.Value.ResetAllocations();
                }

                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();

                if (_buffer != null)
                {
                    ArrayPool<byte>.Shared.Return(_buffer);
                    _buffer = null;
                }
            }
        }

        public virtual void Commit(IndexingStatsScope stats)
        {
            if (_writer != null)
            {
                using (var commitStats = stats.For(IndexingOperation.Lucene.Commit))
                {
                    _writer.Commit(_state, commitStats); // just make sure changes are flushed to disk

                    if (_hasSuggestions)
                    {
                        foreach (var item in _suggestionsWriters)
                            item.Value.Commit(_state);
                    }
                }
            }
        }

        public void Optimize()
        {
            if (_writer != null)
            {
                _writer.Optimize(_state);

                if (_hasSuggestions)
                {
                    foreach (var item in _suggestionsWriters)
                        item.Value.Optimize(_state);
                }
            }
        }

        public virtual void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);

            bool shouldSkip;
            IDisposable setDocument;
            using (Stats.ConvertStats.Start())
                setDocument = _converter.SetDocument(key, sourceDocumentId, document, indexContext, this, out shouldSkip);

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
            }
        }

        public int EntriesCount()
        {
            return _writer.EntriesCount(_state);
        }

        public (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
        {
            var usedMemory = _writer.RamSizeInBytes();
            var fileAllocations = _directory.FilesAllocations;

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

        public virtual void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_sourceDocumentIdHash.CreateTerm(sourceDocumentId), _state);
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

        public byte[] GetBuffer(int necessarySize)
        {
            if (_buffer == null)
                return _buffer = ArrayPool<byte>.Shared.Rent(necessarySize);

            if (_buffer.Length < necessarySize)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                return _buffer = ArrayPool<byte>.Shared.Rent(necessarySize);
            }

            return _buffer;
        }
    }

    public interface IWriteOperationBuffer
    {
        byte[] GetBuffer(int necessarySize);
    }
}
