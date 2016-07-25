using System;
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
using Voron;

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

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToArray(), staticIndex.OutputFields, staticIndex.GroupByFields);
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);

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
                private readonly HashSet<string> _groupByFields;

                public Enumerator(IEnumerator enumerator, AnonymusObjectToBlittableMapResultsEnumerableWrapper parent)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                    _groupByFields = _parent._index.Definition.GroupByFields;
                }

                public bool MoveNext()
                {
                    Current.Data?.Dispose();

                    if (_enumerator.MoveNext() == false)
                        return false;

                    var document = _enumerator.Current;

                    var accessor = _parent._propertyAccessor ?? (_parent._propertyAccessor = PropertyAccessor.Create(document.GetType()));

                    var mapResult = new DynamicJsonValue();
                    ulong reduceHashKey;

                    var hash64Context = Hashing.Streamed.XXHash64.BeginProcess();

                    foreach (var property in accessor.Properties)
                    {
                        var value = property.Value(document);

                        mapResult[property.Key] = value;
                        
                        if (_groupByFields.Contains(property.Key))
                        {
                            AddValueToReduceHash(value, hash64Context);
                        }
                    }

                    reduceHashKey = Hashing.Streamed.XXHash64.EndProcess(hash64Context);

                    Current.Data = _parent._indexContext.ReadObject(mapResult, "map-result");
                    Current.ReduceKeyHash = reduceHashKey;
                    Current.State = _parent._index.GetReduceKeyState(reduceHashKey, _parent._indexContext, create: true);

                    return true;
                }

                private static void AddValueToReduceHash(object value, Hashing.Streamed.XXHash64Context hash64Context)
                {
                    var lsv = value as LazyStringValue;
                    if (lsv != null)
                    {
                        var size = lsv.Size & ~0xF; // we need 16 bytes alignment TODO arek

                        Hashing.Streamed.XXHash64.Process(hash64Context, lsv.Buffer, size);
                        return;
                    }

                    var s = value as string;
                    if (s != null)
                    {
                        fixed (char* p = s)
                        {
                            Hashing.Streamed.XXHash64.Process(hash64Context, (byte*)p,
                                s.Length * sizeof(char));
                        }
                        return;
                    }

                    var lcsv = value as LazyCompressedStringValue;
                    if (lcsv != null)
                    {
                        Hashing.Streamed.XXHash64.Process(hash64Context, lcsv.Buffer, lcsv.CompressedSize);
                        return;
                    }

                    if (value is long)
                    {
                        var l = (long)value;
                        Hashing.Streamed.XXHash64.Process(hash64Context, (byte*)&l, sizeof(long));
                        return;
                    }

                    if (value is decimal)
                    {
                        var l = (decimal)value;
                        Hashing.Streamed.XXHash64.Process(hash64Context, (byte*)&l, sizeof(decimal));
                        return;
                    }

                    throw new NotSupportedException($"Unhandled type: {value.GetType()}");
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