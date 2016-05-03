using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoMapReduceIndexDefinition>
    {
        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private readonly TableSchema _mapResultsSchema = new TableSchema();

        private readonly MapReduceIndexingContext _indexingWorkContext = new MapReduceIndexingContext();

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.AutoMapReduce, definition)
        {
            _mapResultsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                Name = "MapResultEtag",
                StartIndex = 0,
                Count = 1
            });

            _mapResultsSchema.DefineIndex("DocumentKeys", new TableSchema.SchemaIndexDef
            {
                Name = "DocumentKeys",
                Count = 1,
                StartIndex = 1,
                IsGlobal = true
            });

            _mapResultsSchema.DefineFixedSizeIndex("ReduceKeyHashes", new TableSchema.FixedSizeSchemaIndexDef
            {
                IsGlobal = true,
                Name = "ReduceKeyHashes",
                StartIndex = 2
            });
        }

        internal long LastMapResultEtag { get; private set; } = -1;

        public static AutoMapReduceIndex CreateNew(int indexId, AutoMapReduceIndexDefinition definition,
            DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapReduceIndex Open(int indexId, StorageEnvironment environment,
            DocumentDatabase documentDatabase)
        {
            var definition = AutoMapReduceIndexDefinition.Load(environment);
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _indexingWorkContext),
                new ReduceMapResults(Definition, _indexStorage, DocumentDatabase.Metrics, _indexingWorkContext)
            };
        }
        
        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            _indexingWorkContext.MapEntriesTable = GetMapEntriesTable(indexContext.Transaction.InnerTransaction);

            return _indexingWorkContext;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var etagSlice = new Slice((byte*)null, sizeof(long));

            foreach (var mapEntry in GetMapEntriesForDocument(_indexingWorkContext.MapEntriesTable, tombstone.Key))
            {
                ReduceKeyState state;
                if (_indexingWorkContext.StateByReduceKeyHash.TryGetValue(mapEntry.ReduceKeyHash, out state) == false)
                {
                    //TODO: Need better way to handle tree names
                    var tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + mapEntry.ReduceKeyHash);
                    _indexingWorkContext.StateByReduceKeyHash[mapEntry.ReduceKeyHash] = state = new ReduceKeyState(tree);
                }

                var etag = mapEntry.Etag;
                etagSlice.Set((byte*)&etag, sizeof(long));

                state.Tree.Delete(etagSlice);

                _indexingWorkContext.MapEntriesTable.Delete(mapEntry.StorageId);
            }
        }

        public override unsafe void HandleMap(Document document, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope collectionScope)
        {
            var mappedResult = new DynamicJsonValue();
            var reduceKey = new DynamicJsonValue();
            foreach (var indexField in Definition.MapFields.Values)
            {
                switch (indexField.MapReduceOperation)
                {
                    case FieldMapReduceOperation.Count:
                        mappedResult[indexField.Name] = 1;
                        break;
                    case FieldMapReduceOperation.Sum:
                        object fieldValue;
                        _blittableTraverser.TryRead(document.Data, indexField.Name, out fieldValue);

                        var arrayResult = fieldValue as IEnumerable<object>;

                        if (arrayResult == null)
                        {
                            // explicitly adding this even if the value isn't there, as a null
                            mappedResult[indexField.Name] = fieldValue;
                            continue;
                        }

                        double? totalDouble = null;
                        long? totalLong = null;

                        foreach (var item in arrayResult)
                        {
                            if (item == null)
                                continue;

                            double doubleValue;
                            long longValue;

                            switch (BlittableNumber.Parse(item, out doubleValue, out longValue))
                            {
                                case NumberParseResult.Double:
                                    if (totalDouble == null)
                                        totalDouble = 0;

                                    totalDouble += doubleValue;
                                    break;
                                case NumberParseResult.Long:
                                    if (totalLong == null)
                                        totalLong = 0;

                                    totalLong += longValue;
                                    break;
                            }
                        }

                        if (totalDouble != null)
                            mappedResult[indexField.Name] = totalDouble;
                        else if (totalLong != null)
                            mappedResult[indexField.Name] = totalLong;
                        else
                            mappedResult[indexField.Name] = 0; // TODO arek - long / double ?

                        break;
                    case FieldMapReduceOperation.None:
                        object result;
                        _blittableTraverser.TryRead(document.Data, indexField.Name, out result);

                        // explicitly adding this even if the value isn't there, as a null
                        mappedResult[indexField.Name] = result;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            foreach (var groupByFieldName in Definition.GroupByFields.Keys)
            {
                object result;
                _blittableTraverser.TryRead(document.Data, groupByFieldName, out result);
                // explicitly adding this even if the value isn't there, as a null
                mappedResult[groupByFieldName] = result;
                reduceKey[groupByFieldName] = result;
            }

            ulong reduceHashKey;
            using (var reduceKeyObject = indexContext.ReadObject(reduceKey, document.Key))
            {
                reduceHashKey = Hashing.XXHash64.Calculate(reduceKeyObject.BasePointer, reduceKeyObject.Size);
            }

            ReduceKeyState state;
            if (_indexingWorkContext.StateByReduceKeyHash.TryGetValue(reduceHashKey, out state) == false)
            {
                //TODO: Need better way to handle tree names
                var tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + reduceHashKey);
                _indexingWorkContext.StateByReduceKeyHash[reduceHashKey] = state = new ReduceKeyState(tree);
            }

            using (var mappedresult = indexContext.ReadObject(mappedResult, document.Key))
            {
                PutMappedResult(mappedresult, state, _indexingWorkContext.MapEntriesTable, document.Key, reduceHashKey);
            }

            DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, string[] fieldsToFetch)
        {
            return new MapReduceQueryResultRetriever(indexContext, fieldsToFetch);
        }

        private Table GetMapEntriesTable(Transaction tx)
        {
            _mapResultsSchema.Create(tx, "MapResults");
            var table = new Table(_mapResultsSchema, "MapResults", tx);

            return table;
        }

        public unsafe void PutMappedResult(BlittableJsonReaderObject mappedResult, ReduceKeyState state, Table table, LazyStringValue documentKey, ulong reduceKeyHash)
        {
            var etag = ++LastMapResultEtag; // TODO arek - it seems that etag it useless

            var etagBigEndian = IPAddress.HostToNetworkOrder(etag);

            var hashBigEndian = Bits.SwapBytes(reduceKeyHash);

            var tvb = new TableValueBuilder
            {
                { (byte*) &etagBigEndian , sizeof (long) },
                { documentKey.Buffer, documentKey.Size },
                { (byte*) &hashBigEndian, sizeof(ulong) }
            };
            
            // TODO arek - need to handle updates

            table.Insert(tvb);
            
            var pos = state.Tree.DirectAdd(new Slice((byte*) &etag, sizeof (long)), mappedResult.Size);

            mappedResult.CopyTo(pos);
        }

        public unsafe List<MapEntry> GetMapEntriesForDocument(Table table, LazyStringValue documentKey)
        {
            var result = new List<MapEntry>();

            var documentKeySlice = new Slice(documentKey.Buffer, (ushort) documentKey.Size);

            var seekForwardFrom = table.SeekForwardFrom(_mapResultsSchema.Indexes["DocumentKeys"], documentKeySlice);

            foreach (var seek in seekForwardFrom)
            {
                if (seek.Key.Equals(documentKeySlice) == false)
                    break;

                foreach (var tvr in seek.Results)
                {
                    int _;
                    var ptr = tvr.Read(0, out _);
                    var etag = IPAddress.NetworkToHostOrder(*(long*)ptr);

                    ptr = tvr.Read(2, out _);
                    var reduceKeyHash = Bits.SwapBytes(*(ulong*) ptr);

                    result.Add(new MapEntry
                    {
                        Etag = etag,
                        ReduceKeyHash = reduceKeyHash,
                        StorageId = tvr.Id
                    });
                }
            }

            return result;
        }

        protected override unsafe void LoadValues()
        {
            base.LoadValues();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree("MapResults");

                if (tree == null)
                    return;

                var table = GetMapEntriesTable(tx.InnerTransaction);
                
                if (table.NumberOfEntries == 0)
                    return;

                var tvr = table.SeekLastByPrimaryKey();

                int _;
                var ptr = tvr.Read(0, out _);
                LastMapResultEtag = IPAddress.NetworkToHostOrder(*(long*)ptr);
            }
        }
    }
}