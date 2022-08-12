using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax;
using Raven.Client.Documents.Indexes;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron.Impl;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data.Containers;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexWriteOperation : IndexWriteOperationBase
    {
        public const int MaximumPersistedCapacityOfCoraxWriter = 512;

        // WORKAROUND: RavenDB-18872
        // https://issues.hibernatingrhinos.com/issue/RavenDB-18872
        protected const int DocumentBufferSize = 128 * Sparrow.Global.Constants.Size.Megabyte;

        private readonly IndexWriter _indexWriter;
        private readonly CoraxDocumentConverterBase _converter;
        private readonly IndexFieldsMapping _knownFields;
        private readonly IDisposable _bufferScope;
        private readonly ByteString _buffer;
        private long _entriesCount = 0;

        public CoraxIndexWriteOperation(Index index, Transaction writeTransaction, CoraxDocumentConverterBase converter, Logger logger) : base(index, logger)
        {
            _bufferScope = writeTransaction.Allocator.Allocate(DocumentBufferSize, out _buffer);
            _converter = converter;
            _knownFields = _converter.GetKnownFieldsForWriter();
            try
            {
                _indexWriter = new IndexWriter(writeTransaction, _knownFields);
                _entriesCount = _indexWriter.GetNumberOfEntries();
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
                    _indexWriter.Commit();
                }
            }
        }

        public override void UpdateDocument(string keyFieldName, 
            LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            
            LazyStringValue lowerId;
            ByteStringContext<ByteStringMemoryCache>.InternalScope scope = default;
            ByteString data;
            using (Stats.ConvertStats.Start())
            {
                scope = _converter.SetDocumentFields(key, sourceDocumentId, document, indexContext, out lowerId, out data);
            }
            
            using(scope)
            using (Stats.AddStats.Start())
            {
                if (data.Length == 0)
                {
                    DeleteByField(keyFieldName, key, stats);
                    return;
                }
                
                _indexWriter.Update(keyFieldName, key.AsSpan(), lowerId, data.ToSpan());
            }
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            _entriesCount++;

            LazyStringValue lowerId;
            ByteString data;
            ByteStringContext<ByteStringMemoryCache>.InternalScope scope = default;
            using (Stats.ConvertStats.Start())
            {
                scope = _converter.SetDocumentFields(key, sourceDocumentId, document, indexContext, out lowerId, out data);
            }

            using (scope)
            {
                if (data.Length == 0)
                    return;

                using (Stats.AddStats.Start())
                {
                    _indexWriter.Index(lowerId, data.ToSpan());
                }

                stats.RecordIndexingOutput();
            } 
        }

        public override long EntriesCount() => _entriesCount;

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
            DeleteByField(Constants.Documents.Indexing.Fields.DocumentIdFieldName, key, stats);
        }

        private void DeleteByField(string fieldName, LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);
            using (Stats.DeleteStats.Start())
            {
                if (_indexWriter.TryDeleteEntry(fieldName, key.ToString(CultureInfo.InvariantCulture)))
                {
                    _entriesCount--;
                }
            }
        }

        public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            throw new NotImplementedException();
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            DeleteByField(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, reduceKeyHash, stats);
            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleted document for '{_indexName}'. Reduce key hash: {reduceKeyHash}.");
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
            _bufferScope?.Dispose();
            _indexWriter?.Dispose();
            if (_converter.StringsListForEnumerableScope?.Capacity > MaximumPersistedCapacityOfCoraxWriter)
            {
                //We want to make sure we didn't persist too much memory for our enumerable writer.
                _converter.DoublesListForEnumerableScope = null;
                _converter.LongsListForEnumerableScope = null;
                _converter.StringsListForEnumerableScope = null;
                _converter.BlittableJsonReaderObjectsListForEnumerableScope = null;
            }
            else
            {
                _converter.DoublesListForEnumerableScope?.Clear();
                _converter.LongsListForEnumerableScope?.Clear();
                _converter.StringsListForEnumerableScope?.Clear();
                _converter.BlittableJsonReaderObjectsListForEnumerableScope?.Clear();
            }
        }
    }
}
