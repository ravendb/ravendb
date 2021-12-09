using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Pipeline;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexWriteOperation : IndexWriteOperationBase
    {
        private readonly IndexWriter _indexWriter;
        private readonly CoraxDocumentConverter _converter;
        private readonly Dictionary<Slice, int> _knownFields;
        private int _entriesCount = 0;
        private readonly CoraxRavenPerFieldAnalyzerWrapper _analyzers;
        public CoraxIndexWriteOperation(Index index, Transaction writeTransaction, CoraxDocumentConverter converter, Logger logger) : base(index, logger)
        {
            _converter = converter;
            _knownFields = _converter.GetKnownFields();
            try
            {
                _analyzers = IndexingHelpers.CreateCoraxAnalyzers(index, index.Definition);
                _indexWriter = new IndexWriter(writeTransaction, _analyzers.Analyzers);
                _entriesCount = Convert.ToInt32(_indexWriter.GetNumberOfEntries());
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
        
        public override void Commit(IndexingStatsScope stats)
        {
            if (_indexWriter != null)
            {
                using (stats.For(IndexingOperation.Corax.Commit))
                {
                    _indexWriter.Commit(_knownFields);
                }
            }
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            _entriesCount++;
            Span<byte> data;
            LazyStringValue lowerId;

            using (Stats.ConvertStats.Start())
                data = _converter.InsertDocumentFields(key, sourceDocumentId, document, indexContext, out lowerId);

            using (Stats.AddStats.Start())
                _indexWriter.Index(lowerId, data, _knownFields);

            stats.RecordIndexingOutput();
        }

        public override int EntriesCount() => _entriesCount;

        public override (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
        {
            //todo maciej
            return (1024 * 1024, 1024 * 1024);
        }

        public override void Optimize()
        {
            // Lucene method
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);
            using (Stats.DeleteStats.Start())
                if (_indexWriter.TryDeleteEntry(Constants.Documents.Indexing.Fields.DocumentIdFieldName, key.ToString()))
                {
                    _entriesCount--;
                }
        }

        public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            throw new NotImplementedException();
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            if (_indexWriter.TryDeleteEntry(Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, reduceKeyHash.ToString()))
            {
                _entriesCount--;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;

            Stats.DeleteStats = stats.For(IndexingOperation.Corax.Delete, start: false);
            Stats.AddStats = stats.For(IndexingOperation.Corax.AddDocument, start: false);
            Stats.ConvertStats = stats.For(IndexingOperation.Corax.Convert, start: false);
        }
        
        public override void Dispose()
        {
            _indexWriter?.Dispose();
            _analyzers?.Dispose();
        }
    }
}
