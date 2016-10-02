using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
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
        private readonly HashSet<CollectionName> _referencedCollections = new HashSet<CollectionName>();

        internal readonly StaticIndexBase _compiled;

        private HandleReferences _handleReferences;

        private readonly Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper>();

        private int _maxNumberOfIndexOutputs;
        private int _actualMaxNumberOfIndexOutputs;

        private MapReduceIndex(int indexId, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.MapReduce, definition)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection);
            }
        }

        public override bool HasBoostedFields => _compiled.HasBoostedFields;

        protected override void InitializeInternal()
        {
            _maxNumberOfIndexOutputs = Definition.IndexDefinition.MaxIndexOutputsPerDocument ?? DocumentDatabase.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument;
        }

        public static MapReduceIndex CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static Index Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = StaticMapIndexDefinition.Load(environment);
            var instance = CreateIndexInstance(indexId, definition);

            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        private static MapReduceIndex CreateIndexInstance(int indexId, IndexDefinition definition)
        {
            var staticIndex = IndexAndTransformerCompilationCache.GetIndexInstance(definition);

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToArray(), staticIndex.OutputFields, staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();
            workers.Add(new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, _compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, _mapReduceWorkContext));

            return workers.ToArray();
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_referencedCollections.Count > 0)
                _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection, StaticIndexDocsEnumerator.EnumerationType.Index);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            AnonymusObjectToBlittableMapResultsEnumerableWrapper wrapper;
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymusObjectToBlittableMapResultsEnumerableWrapper(this);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext);

            return PutMapResults(key, wrapper, indexContext);
        }

        public override int? ActualMaxNumberOfIndexOutputs
        {
            get
            {
                if (_actualMaxNumberOfIndexOutputs <= 1)
                    return null;

                return _actualMaxNumberOfIndexOutputs;
            }
        }
        public override int MaxNumberOfIndexOutputs => _maxNumberOfIndexOutputs;
        protected override bool EnsureValidNumberOfOutputsForDocument(int numberOfAlreadyProducedOutputs)
        {
            if (base.EnsureValidNumberOfOutputsForDocument(numberOfAlreadyProducedOutputs) == false)
                return false;

            if (Definition.IndexDefinition.MaxIndexOutputsPerDocument != null)
            {
                // user has specifically configured this value, but we don't trust it.

                if (_actualMaxNumberOfIndexOutputs < numberOfAlreadyProducedOutputs)
                    _actualMaxNumberOfIndexOutputs = numberOfAlreadyProducedOutputs;
            }

            return true;
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        private class AnonymusObjectToBlittableMapResultsEnumerableWrapper : IEnumerable<MapResult>
        {
            private readonly MapReduceIndex _index;
            private IEnumerable _items;
            private TransactionOperationContext _indexContext;
            private PropertyAccessor _propertyAccessor;
            private readonly ReduceKeyProcessor _reduceKeyProcessor;
            private readonly HashSet<string> _fields;
            private readonly HashSet<string> _groupByFields;

            public AnonymusObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index)
            {
                _index = index;
                _fields = new HashSet<string>(_index.Definition.MapFields.Keys);
                _groupByFields = _index.Definition.GroupByFields;
                _reduceKeyProcessor = new ReduceKeyProcessor(_index.Definition.GroupByFields.Count, _index._unmanagedBuffersPool);
            }

            public void InitializeForEnumeration(IEnumerable items, TransactionOperationContext indexContext)
            {
                _items = items;
                _indexContext = indexContext;
            }

            public IEnumerator<MapResult> GetEnumerator()
            {
                return new Enumerator(_items.GetEnumerator(), this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }


            private class Enumerator : IEnumerator<MapResult>
            {
                private readonly IEnumerator _enumerator;
                private readonly AnonymusObjectToBlittableMapResultsEnumerableWrapper _parent;
                private readonly HashSet<string> _fields;
                private readonly HashSet<string> _groupByFields;
                private readonly ReduceKeyProcessor _reduceKeyProcessor;

                public Enumerator(IEnumerator enumerator, AnonymusObjectToBlittableMapResultsEnumerableWrapper parent)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                    _groupByFields = _parent._groupByFields;
                    _fields = _parent._fields;
                    _reduceKeyProcessor = _parent._reduceKeyProcessor;
                }

                public bool MoveNext()
                {
                    if (_enumerator.MoveNext() == false)
                        return false;

                    var document = _enumerator.Current;

                    var accessor = _parent._propertyAccessor ?? (_parent._propertyAccessor = PropertyAccessor.Create(document.GetType()));

                    var mapResult = new DynamicJsonValue();

                    _reduceKeyProcessor.Reset();

                    foreach (var field in _fields)
                    {
                        var value = accessor.Properties[field].GetValue(document);
                        var blittableValue = TypeConverter.ToBlittableSupportedType(value, _parent._indexContext);
                        mapResult[field] = blittableValue;

                        if (_groupByFields.Contains(field))
                        {
                            _reduceKeyProcessor.Process(blittableValue);
                        }
                    }

                    var reduceHashKey = _reduceKeyProcessor.Hash;

                    Current.Data = _parent._indexContext.ReadObject(mapResult, "map-result");
                    Current.ReduceKeyHash = reduceHashKey;

                    return true;
                }

                public void Reset()
                {
                    throw new System.NotImplementedException();
                }

                public MapResult Current { get; } = new MapResult();

                object IEnumerator.Current
                {
                    get { return Current; }
                }

                public void Dispose()
                {
                }
            }
        }
    }
}