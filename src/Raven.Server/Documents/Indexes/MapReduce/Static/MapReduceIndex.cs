using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
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
            private readonly ReduceKeyProcessor _reduceKeyProcessor;

            public AnonymusObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index)
            {
                _index = index;
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
                private readonly HashSet<string> _groupByFields;
                private readonly ReduceKeyProcessor _reduceKeyProcessor;

                public Enumerator(IEnumerator enumerator, AnonymusObjectToBlittableMapResultsEnumerableWrapper parent)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                    _groupByFields = _parent._index.Definition.GroupByFields;
                    _reduceKeyProcessor = _parent._reduceKeyProcessor;
                }

                public bool MoveNext()
                {
                    Current.Data?.Dispose();

                    if (_enumerator.MoveNext() == false)
                        return false;

                    var document = _enumerator.Current;

                    var accessor = _parent._propertyAccessor ?? (_parent._propertyAccessor = PropertyAccessor.Create(document.GetType()));

                    var mapResult = new DynamicJsonValue();

                    _reduceKeyProcessor.Init();

                    foreach (var property in accessor.Properties)
                    {
                        var value = property.Value(document);

                        mapResult[property.Key] = value;
                        
                        if (_groupByFields.Contains(property.Key))
                        {
                            _reduceKeyProcessor.Process(value);
                        }
                    }

                    var reduceHashKey = _reduceKeyProcessor.Hash;

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

            private unsafe class ReduceKeyProcessor
            {
                private readonly UnmanagedBuffersPool _buffersPool;
                private readonly Mode _mode;
                private UnmanagedBuffersPool.AllocatedMemoryData _buffer;
                private int _bufferPos;
                private ulong _singleValueHash;

                public ReduceKeyProcessor(int numberOfReduceFields, UnmanagedBuffersPool buffersPool)
                {
                    _buffersPool = buffersPool;
                    if (numberOfReduceFields == 1)
                    {
                        _mode = Mode.SingleValue;
                    }
                    else
                    {
                        _mode = Mode.MultipleValues;
                        _buffer = _buffersPool.Allocate(16);
                        _bufferPos = 0;
                    }
                }

                public void Init()
                {
                    _bufferPos = 0;
                }

                public ulong Hash
                {
                    get
                    {
                        switch (_mode)
                        {
                            case Mode.SingleValue:
                                return _singleValueHash;
                            case Mode.MultipleValues:
                                return Hashing.XXHash64.CalculateInline((byte*)_buffer.Address, _bufferPos);
                            default:
                                throw new NotSupportedException($"Unknown reduce value processing mode: {_mode}");
                        }
                    }
                }

                public void Process(object value)
                {
                    var lsv = value as LazyStringValue;
                    if (lsv != null)
                    {
                        switch (_mode)
                        {
                            case Mode.SingleValue:
                                _singleValueHash = Hashing.XXHash64.Calculate(lsv.Buffer, lsv.Size);
                                break;
                            case Mode.MultipleValues:
                                CopyToBuffer(lsv.Buffer, lsv.Size);
                                break;
                        }

                        return;
                    }

                    var s = value as string;
                    if (s != null)
                    {
                        fixed (char* p = s)
                        {
                            switch (_mode)
                            {
                                case Mode.SingleValue:
                                    _singleValueHash = Hashing.XXHash64.Calculate((byte*)p, s.Length * sizeof(char));
                                    break;
                                case Mode.MultipleValues:
                                    CopyToBuffer((byte*)p, s.Length * sizeof(char));
                                    break;
                            }
                        }
                        
                        return;
                    }

                    var lcsv = value as LazyCompressedStringValue;
                    if (lcsv != null)
                    {
                        switch (_mode)
                        {
                            case Mode.SingleValue:
                                _singleValueHash = Hashing.XXHash64.Calculate(lcsv.Buffer, lcsv.CompressedSize);
                                break;
                            case Mode.MultipleValues:
                                CopyToBuffer(lcsv.Buffer, lcsv.CompressedSize);
                                break;
                        }

                        return;
                    }

                    if (value is long)
                    {
                        var l = (long)value;

                        switch (_mode)
                        {
                            case Mode.SingleValue:
                                _singleValueHash = Hashing.XXHash64.Calculate((byte*)&l, sizeof(long));
                                break;
                            case Mode.MultipleValues:
                                CopyToBuffer((byte*)&l, sizeof(long));
                                break;
                        }

                        return;
                    }

                    if (value is decimal)
                    {
                        var l = (decimal)value;

                        switch (_mode)
                        {
                            case Mode.SingleValue:
                                _singleValueHash = Hashing.XXHash64.Calculate((byte*)&l, sizeof(decimal));
                                break;
                            case Mode.MultipleValues:
                                CopyToBuffer((byte*)&l, sizeof(decimal));
                                break;
                        }

                        return;
                    }

                    throw new NotSupportedException($"Unhandled type: {value.GetType()}"); // TODO arek
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                private void CopyToBuffer(byte* value, int size)
                {
                    if (_bufferPos + size > _buffer.SizeInBytes)
                    {
                        var newBuffer = _buffersPool.Allocate(Bits.NextPowerOf2(_bufferPos + size));
                        Memory.Copy((byte*)newBuffer.Address, (byte*)_buffer.Address, _buffer.SizeInBytes);

                        _buffersPool.Return(_buffer);
                        _buffer = newBuffer;
                    }

                    Memory.Copy((byte*)_buffer.Address + _bufferPos, value, size);
                    _bufferPos += size;
                }

                enum Mode
                {
                    SingleValue,
                    MultipleValues
                }
            }
        }
    }
}