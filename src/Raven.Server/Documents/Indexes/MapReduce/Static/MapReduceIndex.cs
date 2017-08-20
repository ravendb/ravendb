using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly StaticIndexBase Compiled;
        private bool? _isSideBySide;

        private HandleReferences _handleReferences;

        private readonly Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper>();

        public PropertyAccessor OutputReduceToCollectionPropertyAccessor;

        private MapReduceIndex(long etag, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(etag, IndexType.MapReduce, definition)
        {
            Compiled = compiled;

            if (Compiled.ReferencedCollections == null)
                return;

            foreach (var collection in Compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection.Name);
            }
        }

        public override bool HasBoostedFields => Compiled.HasBoostedFields;

        public override bool IsMultiMap => Compiled.Maps.Count > 1 || Compiled.Maps.Any(x => x.Value.Count > 1);

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false &&
                _referencedCollections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }

        public static MapReduceIndex CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(definition);
            ValidateReduceResultsCollectionName(definition, instance.Compiled, documentDatabase);

            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static void ValidateReduceResultsCollectionName(IndexDefinition definition, StaticIndexBase index, DocumentDatabase database)
        {
            var outputReduceToCollection = definition.OutputReduceToCollection;
            if (string.IsNullOrWhiteSpace(outputReduceToCollection))
                return;

            var collections = index.Maps.Keys.ToArray();

            if (collections.Contains(Constants.Documents.Collections.AllDocumentsCollection, StringComparer.OrdinalIgnoreCase))
            {
                throw new IndexInvalidException($"Cannot output documents from index ({definition.Name}) to the collection name ({outputReduceToCollection}) because the index is mapping all documents and this will result in an infinite loop.");
            }
            if (collections.Contains(outputReduceToCollection, StringComparer.OrdinalIgnoreCase))
            {
                throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) cannot be used as this index ({definition.Name}) is mapping this collection and this will result in an infinite loop.");
            }

            var indexes = database.IndexStore.GetIndexes()
                .Where(x => x.Type == IndexType.MapReduce)
                .Cast<MapReduceIndex>()
                .Where(mapReduceIndex => string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false &&
                                         mapReduceIndex.Name != definition.Name)
                .ToList();

            foreach (var otherIndex in indexes)
            {
                if (otherIndex.Definition.OutputReduceToCollection.Equals(outputReduceToCollection, StringComparison.OrdinalIgnoreCase))
                    throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) which will be used to output documents results should be unique to only one index but it is already used by another index ({otherIndex.Name}).");

                if (otherIndex.Collections.Contains(outputReduceToCollection, StringComparer.OrdinalIgnoreCase) &&
    CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(otherIndex, collections, indexes, out string description))
                {
                    description += Environment.NewLine + $"--> {definition.Name}: {string.Join(",", collections)} => *{outputReduceToCollection}*";
                    throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) cannot be used to output documents results as it is consumed by other index that will also output results which will lead to an infinite loop:" + Environment.NewLine + description);
                }
            }
        }

        private static bool CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(
            MapReduceIndex otherIndex, string[] indexCollections,
            List<MapReduceIndex> indexes, out string description)
        {
            description = $"{otherIndex.Name}: {string.Join(",", otherIndex.Collections)} => {otherIndex.Definition.OutputReduceToCollection}";

            if (string.IsNullOrWhiteSpace(otherIndex.Definition.OutputReduceToCollection))
                return false;

            if (indexCollections.Contains(otherIndex.Definition.OutputReduceToCollection))
                return true;

            foreach (var index in indexes.Where(mapReduceIndex => mapReduceIndex.Collections.Contains(otherIndex.Definition.OutputReduceToCollection, StringComparer.OrdinalIgnoreCase)))
            {
                var failed = CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(index, indexCollections, indexes, out string innerDescription);
                description += Environment.NewLine + innerDescription;
                if (failed)
                {
                    return true;
                }
            }

            return false;
        }

        public static Index Open(long etag, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = MapIndexDefinition.Load(environment);
            var instance = CreateIndexInstance(definition);

            instance.Initialize(environment, documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapReduceIndex)index;
            var staticIndex = staticMapIndex.Compiled;

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToHashSet(), staticIndex.OutputFields,
                staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }

        private static MapReduceIndex CreateIndexInstance(IndexDefinition definition)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToHashSet(), staticIndex.OutputFields,
                staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            var instance = new MapReduceIndex(definition.Etag, staticMapIndexDefinition, staticIndex);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();
            workers.Add(new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleReferences(this, Compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, Compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext));

            return workers.ToArray();
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_referencedCollections.Count > 0)
                _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new StaticIndexDocsEnumerator(documents, Compiled.Maps[collection], collection, stats);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out AnonymousObjectToBlittableMapResultsEnumerableWrapper wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymousObjectToBlittableMapResultsEnumerableWrapper(this, indexContext);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext, stats);

            return PutMapResults(key, wrapper, indexContext, stats);
        }

        protected override bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null)
        {
            var isStale = base.IsStale(databaseContext, indexContext, cutoff);
            if (isStale || _referencedCollections.Count == 0)
                return isStale;

            return StaticIndexHelper.IsStale(this, databaseContext, indexContext, cutoff);
        }

        protected override unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            if (_referencedCollections.Count == 0)
                return base.CalculateIndexEtag(isStale, documentsContext, indexContext);

            var minLength = MinimumSizeForCalculateIndexEtagLength();
            var length = minLength +
                         sizeof(long) * 2 * (Collections.Count * _referencedCollections.Count); // last referenced collection etags and last processed reference collection etags

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            var writePos = indexEtagBytes + minLength;

            return StaticIndexHelper.CalculateIndexEtag(this, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        protected override bool ShouldReplace()
        {
            if (_isSideBySide.HasValue == false)
                _isSideBySide = Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase);

            if (_isSideBySide == false)
                return false;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext databaseContext))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                using (indexContext.OpenReadTransaction())
                using (databaseContext.OpenReadTransaction())
                {
                    var canReplace = StaticIndexHelper.CanReplace(this, IsStale(databaseContext, indexContext), DocumentDatabase, databaseContext, indexContext);
                    if (canReplace)
                        _isSideBySide = null;

                    return canReplace;
                }
            }
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return Compiled.ReferencedCollections;
        }

        private class AnonymousObjectToBlittableMapResultsEnumerableWrapper : IEnumerable<MapResult>
        {
            private IEnumerable _items;
            private TransactionOperationContext _indexContext;
            private IndexingStatsScope _stats;
            private IndexingStatsScope _createBlittableResultStats;
            private readonly ReduceKeyProcessor _reduceKeyProcessor;
            private readonly HashSet<string> _groupByFields;
            private readonly bool _isMultiMap;
            private PropertyAccessor _propertyAccessor;
            private readonly StaticIndexBase _compiledIndex;

            public AnonymousObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index, TransactionOperationContext indexContext)
            {
                _indexContext = indexContext;
                _groupByFields = index.Definition.GroupByFields;
                _isMultiMap = index.IsMultiMap;
                _reduceKeyProcessor = new ReduceKeyProcessor(index.Definition.GroupByFields.Count, index._unmanagedBuffersPool);
                _compiledIndex = index.Compiled;
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
                private readonly HashSet<string> _groupByFields;
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

                    using (_createBlittableResult.Start())
                    {
                        PropertyAccessor accessor;

                        if (_parent._isMultiMap == false)
                            accessor = _parent._propertyAccessor ??
                                       (_parent._propertyAccessor = PropertyAccessor.CreateMapReduceOutputAccessor(output.GetType(), _groupByFields));
                        else
                            accessor = TypeConverter.GetPropertyAccessorForMapReduceOutput(output, _groupByFields);

                        var mapResult = new DynamicJsonValue();

                        _reduceKeyProcessor.Reset();

                        var propertiesInOrder = accessor.PropertiesInOrder;
                        int properties = propertiesInOrder.Count;

                        for (int i = 0; i < properties; i++)
                        {
                            var field = propertiesInOrder[i];

                            var value = field.Value.GetValue(output);
                            var blittableValue = TypeConverter.ToBlittableSupportedType(value);
                            mapResult[field.Key] = blittableValue;

                            if (field.Value.IsGroupByField)
                                _reduceKeyProcessor.Process(_parent._indexContext.Allocator, blittableValue);
                        }

                        if (_reduceKeyProcessor.ProcessedFields != _groupByFields.Count)
                            ThrowMissingGroupByFieldsInMapOutput(output, _groupByFields, _parent._compiledIndex);

                        var reduceHashKey = _reduceKeyProcessor.Hash;

                        Current.Data = _parent._indexContext.ReadObject(mapResult, "map-result");
                        Current.ReduceKeyHash = reduceHashKey;
                    }

                    return true;
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

                private static void ThrowMissingGroupByFieldsInMapOutput(object output, HashSet<string> groupByFields, StaticIndexBase compiledIndex)
                {
                    throw new InvalidOperationException(
                        $"The output of the mapping function does not contain all fields that the index is supposed to group by.{Environment.NewLine}" +
                        $"Output: {output}{Environment.NewLine}" +
                        $"Group by fields: {string.Join(",", groupByFields)}{Environment.NewLine}" +
                        $"Compiled index def:{Environment.NewLine}{compiledIndex.Source}");
                }
            }
        }
    }
}
