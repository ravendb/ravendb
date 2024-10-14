using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexWriteOperation : IndexWriteOperationBase
    {
        public const int MaximumPersistedCapacityOfCoraxWriter = 512;
        private readonly IndexWriter _indexWriter;
        private readonly CoraxDocumentConverterBase _converter;
        private readonly IndexFieldsMappingBuilder _dynamicFieldsBuilder;
        private IndexFieldsMapping _dynamicFields;
        private readonly CurrentIndexingScope _indexingScope;
        private readonly ByteStringContext _allocator;

        public CoraxIndexWriteOperation(Index index, Transaction writeTransaction, CoraxDocumentConverterBase converter, Logger logger) : base(index, logger)
        {
            _converter = converter;
            var knownFields = _converter.GetKnownFieldsForWriter();
            _indexingScope = CurrentIndexingScope.Current;
            _allocator = writeTransaction.Allocator;
            try
            {
                _indexWriter = index.Definition.HasDynamicFields 
                    ? new IndexWriter(writeTransaction, knownFields, true) 
                    : new IndexWriter(writeTransaction, knownFields);
            }
            catch (Exception e) when (e.IsOutOfMemory())
            {
                throw;
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
            
            if (index.Definition.HasDynamicFields)
            {
                _dynamicFieldsBuilder = IndexFieldsMappingBuilder.CreateForWriter(true);
                try
                {
                    _dynamicFieldsBuilder
                        .AddDefaultAnalyzer(knownFields.DefaultAnalyzer)
                        .AddExactAnalyzer(knownFields.ExactAnalyzer)
                        .AddSearchAnalyzer(knownFields.SearchAnalyzer);
                }
                catch
                {
                    _dynamicFieldsBuilder.Dispose();
                    throw;
                }
                _indexingScope.DynamicFields ??= new();
                UpdateDynamicFieldsBindings();
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
            float? documentBoost = null;
            using (Stats.ConvertStats.Start())
            {
                scope = _converter.SetDocumentFields(key, sourceDocumentId, document, indexContext, out lowerId, out data, out documentBoost);
            }
            
            if (_dynamicFieldsBuilder != null && _dynamicFieldsBuilder.Count != _indexingScope.CreatedFieldsCount)
            {
                UpdateDynamicFieldsBindings();
            }
            
            using(scope)
            using (Stats.AddStats.Start())
            {
                if (data.Length == 0)
                {
                    DeleteByField(keyFieldName, key, stats);
                    return;
                }

                if (documentBoost.HasValue)
                    _indexWriter.Update(keyFieldName, key.AsSpan(), lowerId, data.ToSpan(), documentBoost.Value);
                else
                    _indexWriter.Update(keyFieldName, key.AsSpan(), lowerId, data.ToSpan());
            }
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            
            LazyStringValue lowerId;
            ByteString data;
            ByteStringContext<ByteStringMemoryCache>.InternalScope scope = default;
            float? documentBoost;
            using (Stats.ConvertStats.Start())
            {
                scope = _converter.SetDocumentFields(key, sourceDocumentId, document, indexContext, out lowerId, out data, out documentBoost);
            }
            
            using (scope)
            {
                if (data.Length == 0)
                    return;

                using (Stats.AddStats.Start())
                {
                    if (documentBoost.HasValue)
                        _indexWriter.Index(lowerId, data.ToSpan(), documentBoost.Value);
                    else
                        _indexWriter.Index(lowerId, data.ToSpan());
                }

                stats.RecordIndexingOutput();
            }
            
            if (_dynamicFieldsBuilder != null && _dynamicFieldsBuilder.Count != _indexingScope.CreatedFieldsCount)
            {
                UpdateDynamicFieldsBindings();
            }
        }

        private void UpdateDynamicFieldsBindings()
        {
            foreach (var (fieldName, fieldIndexing) in _indexingScope.DynamicFields)
            {
                using var _ = Slice.From(_allocator, fieldName, out var slice);
                _dynamicFieldsBuilder.AddDynamicBinding(slice, FieldIndexingIntoFieldIndexingMode(fieldIndexing));
            }

            _dynamicFields = _dynamicFieldsBuilder.Build();
            _indexWriter.UpdateDynamicFieldsMapping(_dynamicFields);
            
            FieldIndexingMode FieldIndexingIntoFieldIndexingMode(FieldIndexing option) => option switch
            {
                FieldIndexing.Search => FieldIndexingMode.Search,
                FieldIndexing.Exact => FieldIndexingMode.Exact,
                FieldIndexing.Default => FieldIndexingMode.Normal,
                FieldIndexing.No => FieldIndexingMode.No,
                _ => throw new ArgumentOutOfRangeException(nameof(option), option, null)
            };
        }

        public override long EntriesCount() => _indexWriter.GetNumberOfEntries();

        public override (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations()
        {
            //todo maciej
            return (1024 * 1024, 1024 * 1024);
        }

        public override void Optimize(CancellationToken token)
        {
            // Lucene method
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            DeleteByField(Constants.Documents.Indexing.Fields.DocumentIdFieldName, key, stats);
        }

        public override void DeleteTimeSeries(LazyStringValue docId, LazyStringValue key, IndexingStatsScope stats)
        {
            // Lucene method
        }

        /// <summary>
        /// Should be called to delete whole entry or entires, not only one field.
        /// </summary>
        private void DeleteByField(string fieldName, LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);
            
            using (Stats.DeleteStats.Start())
                _indexWriter.TryDeleteEntry(fieldName, key.ToString(CultureInfo.InvariantCulture));
        }

        public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);
            
            using (var _ = Stats.DeleteStats.Start())
                _indexWriter.TryDeleteEntry(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, sourceDocumentId.ToString(CultureInfo.InvariantCulture));
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
            _indexWriter?.Dispose();
            _dynamicFieldsBuilder?.Dispose();
            _dynamicFields?.Dispose();
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
