using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition>
    {
        private readonly StaticIndexBase _compiled;
        private readonly Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymusObjectToBlittableMapResultsEnumerableWrapper>();

        private MapReduceIndex(int indexId, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.MapReduce, definition)
        {
            _compiled = compiled;
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToArray(), staticIndex.GroupByFields);
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection);
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