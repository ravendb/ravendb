using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Workers;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition, IndexField>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly AbstractStaticIndexBase _compiled;
        private bool? _isSideBySide;

        protected HandleReferences _handleReferences;
        protected HandleCompareExchangeReferences _handleCompareExchangeReferences;

        protected readonly Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper>();

        public IPropertyAccessor OutputReduceToCollectionPropertyAccessor;

        protected MapReduceIndex(MapReduceIndexDefinition definition, AbstractStaticIndexBase compiled)
            : base(definition.IndexDefinition.Type, definition.IndexDefinition.SourceType, definition)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection.Name);
            }
        }

        public override bool HasBoostedFields => _compiled.HasBoostedFields;

        public override bool IsMultiMap => _compiled.Maps.Count > 1 || _compiled.Maps.Any(x => x.Value.Count > 1 || x.Value.Any(y => y.Value.Count > 1));

        public override void ResetIsSideBySideAfterReplacement()
        {
            _isSideBySide = null;
        }

        public OutputReduceToCollectionActions OutputReduceToCollection { get; private set; }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false &&
                _referencedCollections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }

        protected override void OnInitialization()
        {
            base.OnInitialization();

            if (string.IsNullOrWhiteSpace(Definition.OutputReduceToCollection) == false)
            {
                OutputReduceToCollection = new OutputReduceToCollectionActions(this);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    OutputReduceToCollection.Initialize(tx);

                    tx.Commit();
                }
            }
        }

        public static void ValidateReduceResultsCollectionName(IndexDefinition definition, AbstractStaticIndexBase index, DocumentDatabase database, bool checkIfCollectionEmpty)
        {
            var outputReduceToCollection = definition.OutputReduceToCollection;
            if (string.IsNullOrWhiteSpace(outputReduceToCollection))
                return;

            if (outputReduceToCollection.Equals(definition.PatternReferencesCollectionName, StringComparison.OrdinalIgnoreCase))
                throw new IndexInvalidException($"Collection defined in {nameof(definition.PatternReferencesCollectionName)} must not be the same as in {nameof(definition.OutputReduceToCollection)}. Collection name: '{outputReduceToCollection}'");

            var collections = index.Maps.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
                throw new IndexInvalidException($"It is forbidden to create the '{definition.Name}' index " +
                                                $"which would output reduce results to documents in the '{outputReduceToCollection}' collection, " +
                                                $"as this index is mapping all documents " +
                                                $"and this will result in an infinite loop.");

            foreach (var referencedCollection in index.ReferencedCollections)
            {
                foreach (var collectionName in referencedCollection.Value)
                {
                    collections.Add(collectionName.Name);
                }
            }

            if (collections.Contains(outputReduceToCollection))
                throw new IndexInvalidException($"It is forbidden to create the '{definition.Name}' index " +
                                                $"which would output reduce results to documents in the '{outputReduceToCollection}' collection, " +
                                                $"as this index is mapping or referencing the '{outputReduceToCollection}' collection " +
                                                $"and this will result in an infinite loop.");

            var indexes = database.IndexStore.GetIndexes()
                .Where(x => x.Type.IsStatic() && x.Type.IsMapReduce())
                .Cast<MapReduceIndex>()
                .Where(mapReduceIndex =>
                {
                    // we have handling for side by side indexing with OutputReduceToCollection so we're checking only other indexes

                    string existingIndexName = mapReduceIndex.Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty,
                        StringComparison.OrdinalIgnoreCase);

                    string newIndexName = definition.Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty,
                        StringComparison.OrdinalIgnoreCase);

                    return string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false && string.Equals(existingIndexName, newIndexName, StringComparison.OrdinalIgnoreCase) == false;
                })
                .ToList();

            foreach (var otherIndex in indexes)
            {
                if (otherIndex.Definition.OutputReduceToCollection.Equals(outputReduceToCollection, StringComparison.OrdinalIgnoreCase))
                {
                    throw new IndexInvalidException($"It is forbidden to create the '{definition.Name}' index " +
                                                    $"which would output reduce results to documents in the '{outputReduceToCollection}' collection, " +
                                                    $"as there is another index named '{otherIndex.Name}' " +
                                                    $"which also output reduce results to documents in the same '{outputReduceToCollection}' collection. " +
                                                    $"{nameof(IndexDefinition.OutputReduceToCollection)} must by set to unique value for each index or be null.");
                }

                var otherIndexCollections = new HashSet<string>(otherIndex.Collections);

                foreach (var referencedCollection in otherIndex.GetReferencedCollections())
                {
                    foreach (var collectionName in referencedCollection.Value)
                    {
                        otherIndexCollections.Add(collectionName.Name);
                    }
                }

                if (otherIndexCollections.Contains(outputReduceToCollection) &&
                    CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(otherIndex, collections, indexes, out string description))
                {
                    description += Environment.NewLine + $"--> {definition.Name}: {string.Join(",", collections)} => *{outputReduceToCollection}*";
                    throw new IndexInvalidException($"It is forbidden to create the '{definition.Name}' index " +
                                                    $"which would output reduce results to documents in the '{outputReduceToCollection}' collection, " +
                                                    $"as '{outputReduceToCollection}' collection is consumed by other index in a way that would " +
                                                    $"lead to an infinite loop." +
                                                    Environment.NewLine + description);
                }
            }

            var existingIndexOrSideBySide = database.IndexStore.GetIndexes()
                .Where(x => x.Type.IsStatic() && x.Type.IsMapReduce())
                .Cast<MapReduceIndex>()
                .FirstOrDefault(x =>
                {
                    var name = definition.Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty,
                        StringComparison.OrdinalIgnoreCase);

                    return x.Name.Equals(name, StringComparison.OrdinalIgnoreCase) &&
                           x.Definition.ReduceOutputIndex != null; // legacy index definitions don't have this field - side by side indexing isn't supported then
                });

            if (existingIndexOrSideBySide != null)
            {
                if (definition.OutputReduceToCollection.Equals(existingIndexOrSideBySide.Definition.OutputReduceToCollection, StringComparison.OrdinalIgnoreCase)) // we have handling for side by side indexing with OutputReduceToCollection
                    checkIfCollectionEmpty = false;
            }

            if (checkIfCollectionEmpty)
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var stats = database.DocumentsStorage.GetCollection(outputReduceToCollection, context);
                    if (stats.Count > 0)
                    {
                        throw new IndexInvalidException(
                            $"Index '{definition.Name}' is defined to output the Reduce results to documents in Collection '{outputReduceToCollection}'. " +
                            $"This collection currently has {stats.Count} document{(stats.Count == 1 ? ' ' : 's')}. " +
                            $"All documents in Collection '{stats.Name}' must be deleted first.");
                    }
                }
            }
        }

        private static bool CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(
            MapReduceIndex indexToCheck, HashSet<string> indexCollections,
            List<MapReduceIndex> indexes, out string description)
        {
            description = $"{indexToCheck.Name}: {string.Join(",", indexToCheck.Collections)}";

            var referencedCollections = new HashSet<string>();

            foreach (var referencedCollection in indexToCheck.GetReferencedCollections())
                foreach (var collectionName in referencedCollection.Value)
                {
                    referencedCollections.Add(collectionName.Name);
                }

            if (referencedCollections.Count > 0)
                description += $" (referenced: {string.Join(",", referencedCollections)})";

            description += $" => {indexToCheck.Definition.OutputReduceToCollection}";

            if (string.IsNullOrWhiteSpace(indexToCheck.Definition.OutputReduceToCollection))
                return false;

            if (indexCollections.Contains(indexToCheck.Definition.OutputReduceToCollection))
                return true;

            foreach (var index in indexes)
            {
                var otherIndexCollections = new HashSet<string>(index.Collections);
                foreach (var referencedCollection in index.GetReferencedCollections())
                    foreach (var collectionName in referencedCollection.Value)
                    {
                        otherIndexCollections.Add(collectionName.Name);
                    }
                if (otherIndexCollections.Contains(indexToCheck.Definition.OutputReduceToCollection))
                {
                    var failed = CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(index, indexCollections, indexes, out string innerDescription);
                    description += Environment.NewLine + innerDescription;
                    if (failed)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public static Index Open<TStaticIndex>(StorageEnvironment environment, DocumentDatabase documentDatabase)
            where TStaticIndex : MapReduceIndex
        {
            var definition = MapIndexDefinition.Load(environment, out var version);

            TStaticIndex instance;
            if (typeof(TStaticIndex) == typeof(MapReduceIndex))
                instance = (TStaticIndex)CreateIndexInstance<MapReduceIndex>(definition, documentDatabase.Configuration, version, (staticMapIndexDefinition, staticIndex) => new MapReduceIndex(staticMapIndexDefinition, staticIndex));
            else if (typeof(TStaticIndex) == typeof(MapReduceTimeSeriesIndex))
                instance = (TStaticIndex)(MapReduceIndex)CreateIndexInstance<MapReduceTimeSeriesIndex>(definition, documentDatabase.Configuration, version, (staticMapIndexDefinition, staticIndex) => new MapReduceTimeSeriesIndex(staticMapIndexDefinition, staticIndex));
            else if (typeof(TStaticIndex) == typeof(MapReduceCountersIndex))
                instance = (TStaticIndex)(MapReduceIndex)CreateIndexInstance<MapReduceCountersIndex>(definition, documentDatabase.Configuration, version, (staticMapIndexDefinition, staticIndex) => new MapReduceCountersIndex(staticMapIndexDefinition, staticIndex));
            else
                throw new NotSupportedException($"Not supported index type {typeof(TStaticIndex).Name}");

            instance.Initialize(environment, documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static MapReduceIndex CreateNew<TStaticIndex>(IndexDefinition definition, DocumentDatabase documentDatabase, bool isIndexReset = false)
            where TStaticIndex : MapReduceIndex
        {
            TStaticIndex instance;
            if (typeof(TStaticIndex) == typeof(MapReduceIndex))
                instance = (TStaticIndex)CreateIndexInstance<MapReduceIndex>(definition, documentDatabase.Configuration, IndexDefinitionBase.IndexVersion.CurrentVersion, (staticMapIndexDefinition, staticIndex) => new MapReduceIndex(staticMapIndexDefinition, staticIndex));
            else if (typeof(TStaticIndex) == typeof(MapReduceTimeSeriesIndex))
                instance = (TStaticIndex)(MapReduceIndex)CreateIndexInstance<MapReduceTimeSeriesIndex>(definition, documentDatabase.Configuration, IndexDefinitionBase.IndexVersion.CurrentVersion, (staticMapIndexDefinition, staticIndex) => new MapReduceTimeSeriesIndex(staticMapIndexDefinition, staticIndex));
            else if (typeof(TStaticIndex) == typeof(MapReduceCountersIndex))
                instance = (TStaticIndex)(MapReduceIndex)CreateIndexInstance<MapReduceCountersIndex>(definition, documentDatabase.Configuration, IndexDefinitionBase.IndexVersion.CurrentVersion, (staticMapIndexDefinition, staticIndex) => new MapReduceCountersIndex(staticMapIndexDefinition, staticIndex));
            else
                throw new NotSupportedException($"Not supported index type {typeof(TStaticIndex).Name}");

            ValidateReduceResultsCollectionName(definition, instance._compiled, documentDatabase,
                checkIfCollectionEmpty: isIndexReset == false);

            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapReduceIndex)index;
            var staticIndex = staticMapIndex._compiled;

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields,
                staticIndex.GroupByFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, staticMapIndex.Definition.Version);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }

        private static TStaticIndex CreateIndexInstance<TStaticIndex>(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, Func<MapReduceIndexDefinition, AbstractStaticIndexBase, TStaticIndex> factory)
            where TStaticIndex : MapReduceIndex
        {
            var staticMapIndexDefinition = CreateIndexDefinition(definition, configuration, indexVersion, out var staticIndex);
            return factory(staticMapIndexDefinition, staticIndex);
        }

        private static MapReduceIndexDefinition CreateIndexDefinition(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, out AbstractStaticIndexBase staticIndex)
        {
            staticIndex = IndexCompilationCache.GetIndexInstance(definition, configuration);
            return new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.GroupByFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, indexVersion);
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();

            workers.Add(new CleanupDocumentsForMapReduce(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext));

            if (_compiled.CollectionsWithCompareExchangeReferences.Count > 0)
                workers.Add(_handleCompareExchangeReferences = new HandleCompareExchangeReferences(this, _compiled.CollectionsWithCompareExchangeReferences, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleDocumentReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, _compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext));

            return workers.ToArray();
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleReferencesDelete(_handleReferences, _handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicBlittableJson>(items, filter: null, _compiled.Maps[collection], collection, stats, type);
        }

        public override Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            if (tombstoneType != ITombstoneAware.TombstoneType.Documents)
                return null;

            using (CurrentlyInUse())
            {
                return StaticIndexHelper.GetLastProcessedDocumentTombstonesPerCollection(
                    this, _referencedCollections, Collections, _compiled.ReferencedCollections, _indexStorage);
            }
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out AnonymousObjectToBlittableMapResultsEnumerableWrapper wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymousObjectToBlittableMapResultsEnumerableWrapper(this, indexContext);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext, stats);

            return PutMapResults(indexItem.LowerId, indexItem.Id, wrapper, indexContext, stats);
        }

        internal override bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            var isStale = base.IsStale(queryContext, indexContext, cutoff, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);

            if (isStale == false && OutputReduceToCollection?.HasDocumentsToDelete(indexContext) == true)
            {
                if (indexContext.IgnoreStalenessDueToReduceOutputsToDelete == false)
                {
                    isStale = true;
                    stalenessReasons?.Add($"There are still some reduce output documents to delete from collection '{Definition.OutputReduceToCollection}'. ");
                }
            }

            if (isStale && (stalenessReasons == null || (_handleReferences == null && _handleCompareExchangeReferences == null)))
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = _compiled.OutputFields.ToHashSet();

            var dynamicEntries = GetDynamicEntriesFields(staticEntries)
                .Except(staticEntries)
                .ToArray();

            return (staticEntries, dynamicEntries);
        }

        protected override unsafe long CalculateIndexEtag(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            QueryMetadata query, bool isStale)
        {
            if (_handleReferences == null && _handleCompareExchangeReferences == null)
                return base.CalculateIndexEtag(queryContext, indexContext, query, isStale);

            var minLength = MinimumSizeForCalculateIndexEtagLength(query);
            var length = minLength;

            if (_handleReferences != null)
                length += sizeof(long) * 4 * (Collections.Count * _referencedCollections.Count); // last referenced collection etags (document + tombstone) and last processed reference collection etags (document + tombstone)

            if (_handleCompareExchangeReferences != null)
                length += sizeof(long) * 4 * _compiled.CollectionsWithCompareExchangeReferences.Count; // last referenced collection etags (document + tombstone) and last processed reference collection etags (document + tombstone)

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, State, queryContext, indexContext);
            UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(queryContext, query, length, indexEtagBytes);

            var writePos = indexEtagBytes + minLength;

            return StaticIndexHelper.CalculateIndexEtag(this, length, indexEtagBytes, writePos, queryContext, indexContext);
        }

        protected override IndexingState GetIndexingStateInternal(QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            var result = base.GetIndexingStateInternal(queryContext, indexContext);
            if (_handleCompareExchangeReferences == null)
                return result;

            (result.LastProcessedCompareExchangeReferenceEtag, result.LastProcessedCompareExchangeReferenceTombstoneEtag) = StaticIndexHelper.GetLastProcessedCompareExchangeReferenceEtags(this, _compiled, indexContext);

            return result;
        }

        public bool IsSideBySide()
        {
            if (_isSideBySide.HasValue)
                return _isSideBySide.Value;

            return Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase);
        }

        protected override bool ShouldReplace()
        {
            return StaticIndexHelper.ShouldReplace(this, ref _isSideBySide);
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        public class AnonymousObjectToBlittableMapResultsEnumerableWrapper : IEnumerable<MapResult>
        {
            private IEnumerable _items;
            private TransactionOperationContext _indexContext;
            private IndexingStatsScope _stats;
            private IndexingStatsScope _createBlittableResultStats;
            private readonly ReduceKeyProcessor _reduceKeyProcessor;
            private readonly HashSet<CompiledIndexField> _groupByFields;
            private readonly bool _isMultiMap;
            private IPropertyAccessor _propertyAccessor;
            private readonly AbstractStaticIndexBase _compiledIndex;

            public AnonymousObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index, TransactionOperationContext indexContext)
            {
                _indexContext = indexContext;
                _groupByFields = index.Definition.GroupByFields;
                _isMultiMap = index.IsMultiMap;
                _reduceKeyProcessor = new ReduceKeyProcessor(index.Definition.GroupByFields.Count, index._unmanagedBuffersPool);
                _compiledIndex = index._compiled;
            }

            public void InitializeForEnumeration(IEnumerable items, TransactionOperationContext indexContext, IndexingStatsScope stats)
            {
                _items = items;
                _indexContext = indexContext;

                if (_stats == stats)
                    return;

                _stats = stats;
                _createBlittableResultStats = _stats.For(IndexingOperation.Reduce.CreateBlittableJson, start: false);
            }

            IEnumerator<MapResult> IEnumerable<MapResult>.GetEnumerator()
            {
                return GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private Enumerator GetEnumerator()
            {
                return new Enumerator(_items.GetEnumerator(), this, _createBlittableResultStats);
            }

            private class Enumerator : IEnumerator<MapResult>
            {
                private readonly IEnumerator _enumerator;
                private readonly AnonymousObjectToBlittableMapResultsEnumerableWrapper _parent;
                private readonly IndexingStatsScope _createBlittableResult;
                private readonly HashSet<CompiledIndexField> _groupByFields;
                private readonly ReduceKeyProcessor _reduceKeyProcessor;

                public Enumerator(IEnumerator enumerator, AnonymousObjectToBlittableMapResultsEnumerableWrapper parent, IndexingStatsScope createBlittableResult)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                    _createBlittableResult = createBlittableResult;
                    _groupByFields = _parent._groupByFields;
                    _reduceKeyProcessor = _parent._reduceKeyProcessor;
                }

                public bool MoveNext()
                {
                    if (_enumerator.MoveNext() == false)
                        return false;

                    var output = _enumerator.Current;

                    _parent._indexContext.CachedProperties.NewDocument();

                    using (_createBlittableResult.Start())
                    using (var writer = new BlittableJsonWriter(_parent._indexContext))
                    {
                        IPropertyAccessor accessor;

                        if (_parent._isMultiMap == false)
                            accessor = _parent._propertyAccessor ??
                                       (_parent._propertyAccessor = PropertyAccessor.CreateMapReduceOutputAccessor(output.GetType(), output, _groupByFields));
                        else
                            accessor = TypeConverter.GetPropertyAccessorForMapReduceOutput(output, _groupByFields);

                        _reduceKeyProcessor.Reset();

                        writer.WriteStartObject();
                        foreach (var property in accessor.GetPropertiesInOrder(output))
                        {
                            var value = property.Value;
                            var blittableValue = TypeConverter.ToBlittableSupportedType(value, context: _parent._indexContext);

                            writer.WritePropertyName(property.Key);
                            WriteValue(writer, blittableValue);

                            if (property.IsGroupByField)
                            {
                                var valueForProcessor = property.GroupByField.GetValue(value, blittableValue);
                                _reduceKeyProcessor.Process(_parent._indexContext.Allocator, valueForProcessor);
                            }
                        }

                        writer.WriteEndObject();

                        if (_reduceKeyProcessor.ProcessedFields != _groupByFields.Count)
                            ThrowMissingGroupByFieldsInMapOutput(output, _groupByFields, _parent._compiledIndex);

                        var reduceHashKey = _reduceKeyProcessor.Hash;

                        writer.FinalizeDocument();

                        Current.Data = writer.CreateReader();
                        Current.ReduceKeyHash = reduceHashKey;
                    }

                    return true;

                    static void WriteValue(BlittableJsonWriter writer, object value)
                    {
                        switch (value)
                        {
                            case bool val:
                                writer.WriteValue(val);
                                break;
                            case decimal val:
                                writer.WriteValue(val);
                                break;
                            case double val:
                                writer.WriteValue(val);
                                break;
                            case float val:
                                writer.WriteValue(val);
                                break;
                            case LazyCompressedStringValue val:
                                writer.WriteValue(val);
                                break;
                            case LazyNumberValue val:
                                writer.WriteValue(val);
                                break;
                            case LazyStringValue val:
                                writer.WriteValue(val);
                                break;
                            case long val:
                                writer.WriteValue(val);
                                break;
                            case int val:
                                writer.WriteValue(val);
                                break;
                            case string val:
                                writer.WriteValue(val);
                                break;
                            case ulong val:
                                writer.WriteValue(val);
                                break;
                            case uint val:
                                writer.WriteValue(val);
                                break;
                            case short val:
                                writer.WriteValue(val);
                                break;
                            case byte val:
                                writer.WriteValue(val);
                                break;
                            case DateTime val:
                                writer.WriteValue(val);
                                break;
                            case DateTimeOffset val:
                                writer.WriteValue(val);
                                break;
                            case TimeSpan val:
                                writer.WriteValue(val);
                                break;
                            case BlittableJsonReaderObject val:
                                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                                writer.WriteStartObject();
                                for (int i = 0; i < val.Count; i++)
                                {
                                    val.GetPropertyByIndex(i, ref propertyDetails);
                                    writer.WritePropertyName(propertyDetails.Name);
                                    WriteValue(writer, propertyDetails.Value);
                                }

                                writer.WriteEndObject();

                                break;
                            case BlittableJsonReaderArray val:
                                writer.WriteStartArray();
                                foreach (var property in val)
                                    WriteValue(writer, property);
                                writer.WriteEndArray();
                                break;
                            case null:
                                writer.WriteNull();
                                break;
                            case DynamicJsonValue val:
                                writer.WriteStartObject();
                                foreach (var property in val.Properties)
                                {
                                    writer.WritePropertyName(property.Name);
                                    WriteValue(writer, property.Value);
                                }
                                writer.WriteEndObject();
                                break;
                            case DynamicJsonArray val:
                                writer.WriteStartArray();
                                foreach (var item in val)
                                    WriteValue(writer, item);
                                writer.WriteEndArray();
                                break;
                            default:
                                throw new NotSupportedException($"Not supported value type '{value?.GetType().Name}'.");
                        }
                    }
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public MapResult Current { get; } = new MapResult();

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _reduceKeyProcessor.ReleaseBuffer();
                }

                private static void ThrowMissingGroupByFieldsInMapOutput(object output, HashSet<CompiledIndexField> groupByFields, AbstractStaticIndexBase compiledIndex)
                {
                    throw new InvalidOperationException(
                        $"The output of the mapping function does not contain all fields that the index is supposed to group by.{Environment.NewLine}" +
                        $"Output: {output}{Environment.NewLine}" +
                        $"Group by fields: {string.Join(",", groupByFields.Select(x => x.Name))}{Environment.NewLine}" +
                        $"Compiled index def:{Environment.NewLine}{compiledIndex.Source}");
                }
            }
        }
    }
}
