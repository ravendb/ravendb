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
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition>
    {
        internal readonly StaticIndexBase _compiled;
        private readonly Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper>();

        private int _maxNumberOfIndexOutputs;
        private int _actualMaxNumberOfIndexOutputs;

        private MapReduceIndex(int indexId, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.MapReduce, definition)
        {
            _compiled = compiled;
        }

        protected override void InitializeInternal()
        {
            _maxNumberOfIndexOutputs = Definition.IndexDefinition.MaxIndexOutputsPerDocument ?? DocumentDatabase.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument;
        }

        public static MapReduceIndex CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexAndTransformerCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToArray(), staticIndex.GroupByFields);
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(documentDatabase);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext),
                new ReduceMapResultsOfStaticIndex(_compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, _mapReduceWorkContext), 
            };
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection, StaticIndexDocsEnumerator.EnumerationType.Index);
        }

        public override void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            AnonymusObjectToBlittableMapResultsEnumerableWrapper wrapper;
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymusObjectToBlittableMapResultsEnumerableWrapper(this);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext);

            PutMapResults(key, wrapper, indexContext);
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

        private class AnonymusObjectToBlittableMapResultsEnumerableWrapper : IEnumerable<MapResult>
        {
            private readonly MapReduceIndex _index;
            private IEnumerable _items;
            private TransactionOperationContext _indexContext;
            private PropertyAccessor _propertyAccessor;

            public AnonymusObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index)
            {
                _index = index;
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


            private unsafe class Enumerator : IEnumerator<MapResult>
            {
                private readonly IEnumerator _enumerator;
                private readonly AnonymusObjectToBlittableMapResultsEnumerableWrapper _parent;

                public Enumerator(IEnumerator enumerator, AnonymusObjectToBlittableMapResultsEnumerableWrapper parent)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                }

                public bool MoveNext()
                {
                    Current.Data?.Dispose();

                    if (_enumerator.MoveNext() == false)
                        return false;

                    var document = _enumerator.Current;

                    var accessor = _parent._propertyAccessor ?? (_parent._propertyAccessor = PropertyAccessor.Create(document.GetType()));

                    var mapResult = new DynamicJsonValue();
                    var reduceKey = new DynamicJsonValue();

                    foreach (var property in accessor.Properties)
                    {
                        var value = property.Value(document);

                        mapResult[property.Key] = value;

                        if (_parent._index.Definition.GroupByFields.Contains(property.Key))
                            reduceKey[property.Key] = value;
                    }

                    ulong reduceHashKey;
                    using (var reduceKeyObject = _parent._indexContext.ReadObject(reduceKey, "reduce-key"))
                    {
                        reduceHashKey = Hashing.XXHash64.Calculate(reduceKeyObject.BasePointer, reduceKeyObject.Size);
                    }

                    Current.Data = _parent._indexContext.ReadObject(mapResult, "map-result");
                    Current.ReduceKeyHash = reduceHashKey;
                    Current.State = _parent._index.GetReduceKeyState(reduceHashKey, _parent._indexContext, create: true);

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
                    Current.Data?.Dispose();
                }
            }
        }
    }
}