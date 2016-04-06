using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Queries.Results;
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

        internal long _lastMapResultEtag = -1;

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.AutoMapReduce, definition)
        {
            _mapResultsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                Name = "MapResultEtag",
                StartIndex = 0,
                Count = 1
            });

            _mapResultsSchema.DefineIndex("DocumentKeys", new TableSchema.SchemaIndexDef()
            {
                Name = "DocumentKeys",
                Count = 1,
                StartIndex = 1,
                IsGlobal = true
            });

            _mapResultsSchema.DefineFixedSizeIndex("ReduceKeyHashes", new TableSchema.FixedSizeSchemaIndexDef()
            {
                IsGlobal = true,
                Name = "ReduceKeyHashes",
                StartIndex = 2
            });
        }

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
            throw new NotImplementedException();
            //var definition = AutoMapIndexDefinition.Load(environment);
            //var instance = new AutoMapReduceIndex(indexId, definition);
            //instance.Initialize(environment, documentDatabase);

            //return instance;
        }
        
        public override void DoIndexingWork(IndexingBatchStats stats, CancellationToken cancellationToken)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenWriteTransaction())
            {
                var stateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();

                ExecuteCleanup(cancellationToken, databaseContext, indexContext, stateByReduceKeyHash);
                ExecuteMap(stats, cancellationToken, databaseContext, indexContext, stateByReduceKeyHash);

                new ReducingExecuter(stateByReduceKeyHash, indexContext, IndexPersistence, DocumentDatabase.Metrics).Execute(cancellationToken);

                tx.Commit();
            }
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return new MapReduceQueryResultRetriever(indexContext);
        }

        private unsafe void ExecuteMap(IndexingBatchStats stats, CancellationToken cancellationToken, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Dictionary<ulong, ReduceKeyState> stateByReduceKeyHash)
        {
            //TODO arek: use stats
            
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            var table = GetMapResultsTable(indexContext.Transaction.InnerTransaction);

            foreach (var collection in Collections)
            {
                var lastMappedEtag = _indexStorage.ReadLastMappedEtag(indexContext.Transaction, collection);

                cancellationToken.ThrowIfCancellationRequested();

                var lastEtag = lastMappedEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();

                using (databaseContext.OpenReadTransaction())
                {
                    foreach (var document in DocumentDatabase.DocumentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag + 1, 0, pageSize))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        count++;
                        lastEtag = document.Etag;

                        var mappedResult = new DynamicJsonValue();
                        var reduceKey = new DynamicJsonValue();
                        foreach (var indexField in Definition.MapFields.Values)
                        {
                            object result;
                            _blittableTraverser.TryRead(document.Data, indexField.Name, out result);
                            // explicitly adding this even if the value isn't there, as a null
                            switch (indexField.MapReduceOperation)
                            {
                                case FieldMapReduceOperation.Count:
                                    mappedResult[indexField.Name] = 1;
                                    break;
                                case FieldMapReduceOperation.None:
                                case FieldMapReduceOperation.Sum:
                                    mappedResult[indexField.Name] = result;
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }
                        }

                        foreach (var indexField in Definition.GroupByFields)
                        {
                            object result;
                            _blittableTraverser.TryRead(document.Data, indexField.Name, out result);
                            // explicitly adding this even if the value isn't there, as a null
                            mappedResult[indexField.Name] = result;
                            reduceKey[indexField.Name] = result;
                        }

                        ulong reduceHashKey;
                        using (var reduceKeyObject = indexContext.ReadObject(reduceKey, document.Key))
                        {
                            reduceHashKey = Hashing.XXHash64.Calculate(reduceKeyObject.BasePointer, reduceKeyObject.Size);
                        }

                        ReduceKeyState state;
                        if (stateByReduceKeyHash.TryGetValue(reduceHashKey, out state) == false)
                        {
                            //TODO: Need better way to handle tree names
                            var tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + reduceHashKey);
                            stateByReduceKeyHash[reduceHashKey] = state = new ReduceKeyState(tree);
                        }
                        using (var mappedresult = indexContext.ReadObject(mappedResult, document.Key))
                        {
                            PutMappedResult(mappedresult, state, table, document.Key, reduceHashKey);
                        }

                        DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark();

                        if (sw.Elapsed > (Debugger.IsAttached == false ? DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15)))
                        {
                            break;
                        }
                    }
                }

                if (count == 0)
                    continue;

                if (lastEtag <= lastMappedEtag)
                    continue;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing map for '{Name} ({IndexId})'. Processed {count} documents in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                _indexStorage.WriteLastMappedEtag(indexContext.Transaction, collection, lastEtag);

                _mre.Set(); // might be more
            }
        }

        private Table GetMapResultsTable(Transaction tx)
        {
            _mapResultsSchema.Create(tx, "MapResults");
            var table = new Table(_mapResultsSchema, "MapResults", tx);

            return table;
        }

        public unsafe void PutMappedResult(BlittableJsonReaderObject mappedResult, ReduceKeyState state, Table table, LazyStringValue documentKey, ulong reduceKeyHash)
        {
            var etag = ++_lastMapResultEtag;

            var etagBigEndian = IPAddress.HostToNetworkOrder(etag);

            var hashBigEndian = Bits.SwapBytes(reduceKeyHash);

            var tvb = new TableValueBuilder
            {
                { (byte*) &etagBigEndian , sizeof (long) },
                { documentKey.Buffer, documentKey.Size },
                { (byte*) &hashBigEndian, sizeof(ulong) }
            };

            table.Insert(tvb);
            
            var pos = state.Tree.DirectAdd(new Slice((byte*) &etag, sizeof (long)), mappedResult.Size);

            mappedResult.CopyTo(pos);
        }

        private unsafe void ExecuteCleanup(CancellationToken token, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Dictionary<ulong, ReduceKeyState> stateByReduceKeyHash)
        {
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            foreach (var collection in Collections)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Collection: {collection}.");

                long lastMappedEtag;
                long lastTombstoneEtag;
                lastMappedEtag = _indexStorage.ReadLastMappedEtag(indexContext.Transaction, collection);
                lastTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. LastMappedEtag: {lastMappedEtag}. LastTombstoneEtag: {lastTombstoneEtag}.");

                var lastEtag = lastTombstoneEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();

                using (var indexWriter = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction))
                using (databaseContext.OpenReadTransaction())
                {
                    foreach (var tombstone in DocumentDatabase.DocumentsStorage.GetTombstonesAfter(databaseContext, collection, lastEtag + 1, 0, pageSize))
                    {
                        token.ThrowIfCancellationRequested();

                        if (Log.IsDebugEnabled)
                            Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Processing tombstone {tombstone.Key} ({tombstone.Etag}).");

                        count++;
                        lastEtag = tombstone.Etag;

                        if (tombstone.DeletedEtag > lastMappedEtag)
                            continue; // no-op, we have not yet indexed this document

                        var etagSlice = new Slice((byte*)null, sizeof(long));

                        foreach (var mapEntry in GetMapEntriesForDocument(indexContext.Transaction.InnerTransaction, tombstone.Key))
                        {
                            ReduceKeyState state;
                            if (stateByReduceKeyHash.TryGetValue(mapEntry.ReduceKeyHash, out state) == false)
                            {
                                //TODO: Need better way to handle tree names
                                var tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + mapEntry.ReduceKeyHash);
                                stateByReduceKeyHash[mapEntry.ReduceKeyHash] = state = new ReduceKeyState(tree);
                            }

                            var etag = mapEntry.Etag;
                            etagSlice.Set((byte*)&etag, sizeof(long));
                            state.Tree.Delete(etagSlice);

                            indexWriter.DeleteReduceResult(mapEntry.ReduceKeyHash);
                        }

                        if (sw.Elapsed > (Debugger.IsAttached == false ? DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15)))
                        {
                            break;
                        }
                    }
                }

                if (count == 0)
                    return;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Processed {count} tombstones in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                if (lastEtag <= lastTombstoneEtag)
                    return;

                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, collection, lastEtag);

                _mre.Set(); // might be more
            }
        }

        public unsafe List<MapEntry> GetMapEntriesForDocument(Transaction tx, LazyStringValue documentKey)
        {
            var result = new List<MapEntry>();

            var table = GetMapResultsTable(tx);

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
                        ReduceKeyHash = reduceKeyHash
                    });
                }
            }

            return result;
        }

        protected override void LoadValues()
        {
            base.LoadValues();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree("MapResults");

                if (tree == null)
                    return;

                throw new NotImplementedException("TODO arek - load last etag");

                using (var it = tree.Iterate())
                {
                    var seek = it.Seek(Slice.AfterAllKeys);

                    var currentKey = it.CurrentKey;
                }
            }
        }
    }
}