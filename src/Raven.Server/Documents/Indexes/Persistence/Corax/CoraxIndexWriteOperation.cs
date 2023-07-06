using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
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
                _indexWriter =  new IndexWriter(writeTransaction, knownFields);
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
                using (stats.For(IndexingOperation.Corax.Prepare))
                {
                    _indexWriter.Prepare();
                }
                using (stats.For(IndexingOperation.Corax.Commit))
                {
                    _indexWriter.Commit();
                }
            }
        }

        public override void UpdateDocument(
            LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            using var builder = _indexWriter.Index(key.AsSpan());

            bool shouldSkip;
            using (Stats.ConvertStats.Start())
            {
                _converter.SetDocument(key, sourceDocumentId, document, indexContext, builder, out _, out shouldSkip);
            }

            if (_dynamicFieldsBuilder != null && _dynamicFieldsBuilder.Count != _indexingScope.CreatedFieldsCount)
            {
                UpdateDynamicFieldsBindings();
            }

            using (Stats.AddStats.Start())
            {
                if (shouldSkip)
                {
                    Delete(key, stats);
                    return;
                }

                builder.Index();
                stats.RecordIndexingOutput();
            }
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext)
        {
            EnsureValidStats(stats);
            using var builder = _indexWriter.Index(key.AsSpan());

            bool shouldSkip;
            using (Stats.ConvertStats.Start())
            {
                _converter.SetDocument(key, sourceDocumentId, document, indexContext, builder, out _, out shouldSkip);
            }

            using (Stats.AddStats.Start())
            {
                if (_dynamicFieldsBuilder != null && _dynamicFieldsBuilder.Count != _indexingScope.CreatedFieldsCount)
                {
                    UpdateDynamicFieldsBindings();
                }

                if (shouldSkip)
                    return;

                builder.Index();
                stats.RecordIndexingOutput();
            }
        }

        private void UpdateDynamicFieldsBindings()
        {
            foreach (var (fieldName, fieldIndexing) in _indexingScope.DynamicFields)
            {
                using var _ = Slice.From(_allocator, fieldName, out var slice);
                _dynamicFieldsBuilder.AddDynamicBinding(slice, FieldIndexingIntoFieldIndexingMode(fieldIndexing.Indexing), fieldIndexing.Storage== FieldStorage.Yes);
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
            EnsureValidStats(stats);
            
            using (Stats.DeleteStats.Start())
                _indexWriter.TryDeleteEntry(key.ToString(CultureInfo.InvariantCulture));
        }
        
        /// <summary>
        /// Should be called to delete whole entry or entires, not only one field.
        /// </summary>
        private void DeleteByField(string fieldName, LazyStringValue key, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);
            
            using (Stats.DeleteStats.Start())
                _indexWriter.TryDeleteEntryByField(fieldName, key.ToString(CultureInfo.InvariantCulture));
        }

        public override void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (var _ = Stats.DeleteStats.Start())
            {
                _indexWriter.TryDeleteEntryByField(
                    Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName,
                    sourceDocumentId.ToString(CultureInfo.InvariantCulture));
            }
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
