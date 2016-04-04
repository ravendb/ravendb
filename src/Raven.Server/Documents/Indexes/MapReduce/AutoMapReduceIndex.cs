using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoMapReduceIndexDefinition>
    {
        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.AutoMapReduce, definition)
        {
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
                var afterMapState = ExecuteMap(stats, cancellationToken, databaseContext, indexContext);

                new ReducingExecuter(afterMapState, indexContext, IndexPersistence, DocumentDatabase.Metrics).Execute(cancellationToken);

                tx.Commit();
            }
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return new MapResultQueryRetriever(indexContext);
        }

        private unsafe Dictionary<ulong, ReduceKeyState> ExecuteMap(IndexingBatchStats stats, CancellationToken cancellationToken, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            //TODO arek: use stats

            var stateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

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
                        //TODO: generate etag values
                        //TODO: associate doc id with the etag value
                        //TODO: associate doc id with all the reduce keys
                        ReduceKeyState state;
                        if (stateByReduceKeyHash.TryGetValue(reduceHashKey, out state) == false)
                        {
                            //TODO: Need better way to handle tree names
                            var tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + reduceHashKey);
                            stateByReduceKeyHash[reduceHashKey] = state = new ReduceKeyState(tree);
                        }
                        using (var mappedresult = indexContext.ReadObject(mappedResult, document.Key))
                        {
                            //TODO: use etags as the key?
                            var pos = state.Tree.DirectAdd(new Slice(document.Key.Buffer, (ushort) document.Key.Size),
                                mappedresult.Size);
                            mappedresult.CopyTo(pos);
                        }

                        DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark();

                        if (sw.Elapsed > DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan)
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

            return stateByReduceKeyHash;
        }
    }
}