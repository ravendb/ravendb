﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;
using Lock = Lucene.Net.Store.Lock;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexWriteOperation : IndexWriteOperationBase, IWriteOperationBuffer
    {
        private readonly Term _documentId = new Term(Constants.Documents.Indexing.Fields.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, "Dummy");
        private readonly Term _sourceDocumentIdHash = new Term(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, "Dummy");
        protected byte[] _buffer;

        protected readonly LuceneRavenPerFieldAnalyzerWrapper _analyzer;
        protected readonly LuceneIndexWriter _writer;
        protected readonly Dictionary<string, LuceneSuggestionIndexWriter> _suggestionsWriters;
        private readonly bool _hasSuggestions;

        private readonly LuceneDocumentConverterBase _converter;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;


        private readonly IState _state;
        private readonly LuceneVoronDirectory _directory;


        public LuceneIndexWriteOperation(Index index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
            : base(index, RavenLogManager.Instance.GetLoggerForIndex<LuceneIndexWriteOperation>(index))
        {
            _converter = converter;


            try
            {
                _analyzer = LuceneIndexingHelpers.CreateLuceneAnalyzer(index, index.Definition);
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

        public override void Commit(IndexingStatsScope stats)
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

        public override void Optimize(CancellationToken token)
        {
            if (_writer != null)
            {
                _writer.Optimize(_state, token);

                if (_hasSuggestions)
                {
                    foreach (var item in _suggestionsWriters)
                        item.Value.Optimize(_state, token);
                }
            }
        }

        public override void UpdateDocument( LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            throw new NotSupportedException();
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
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

        public override long EntriesCount()
        {
            return _writer.EntriesCount(_state);
        }

        public override (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
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

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_documentId.CreateTerm(key), _state);
        }

        public override void DeleteByPrefix(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
            {
                var term = _documentId.CreateTerm(key);
                _writer.DeleteByPrefix(term, _state);
            }
        }

        public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_sourceDocumentIdHash.CreateTerm(sourceDocumentId), _state);
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Stats.DeleteStats.Start())
                _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash), _state);

            if (_logger.IsDebugEnabled)
                _logger.Debug($"Deleted document for '{_indexName}'. Reduce key hash: {reduceKeyHash}.");
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
