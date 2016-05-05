using System;
using System.Collections.Generic;
using System.Diagnostics;

using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoMapReduceIndexDefinition>
    {
        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private readonly MapReduceIndexingContext _mapReduceWorkContext = new MapReduceIndexingContext();

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
            var definition = AutoMapReduceIndexDefinition.Load(environment);
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, _mapReduceWorkContext),
                new ReduceMapResults(Definition, _indexStorage, DocumentDatabase.Metrics, _mapReduceWorkContext)
            };
        }
        
        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            _mapReduceWorkContext.MapEntries = GetMapEntriesTree(indexContext.Transaction.InnerTransaction);

            return _mapReduceWorkContext;
        }

        public override IEnumerable<object> EnumerateMap(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return documents;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var documentMapEntries = _mapReduceWorkContext.MapEntries.FixedTreeFor(tombstone.Key, sizeof(ulong));

            if (documentMapEntries.NumberOfEntries == 0)
                return;

            foreach (var mapEntry in GetMapEntriesForDocument(tombstone.Key, documentMapEntries))
            {
                var state = GetReduceKeyState(mapEntry.ReduceKeyHash, indexContext, create: false);
                
                fixed (long* ptr = &mapEntry.Id)
                    state.Tree.Delete(Slice.External(indexContext.Allocator, (byte*)ptr, sizeof(long)));

                _mapReduceWorkContext.EntryDeleted(mapEntry);
            }

            _mapReduceWorkContext.MapEntries.DeleteFixedTreeFor(tombstone.Key, sizeof(ulong));
        }

        public override unsafe void HandleMap(LazyStringValue key, object doc, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope collectionScope)
        {
            var document = (Document)doc;
            Debug.Assert(key == document.Key);

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

            var state = GetReduceKeyState(reduceHashKey, indexContext, create: true);

            using (var mappedresult = indexContext.ReadObject(mappedResult, document.Key))
            {
                PutMappedResult(mappedresult, document.Key, reduceHashKey, state, indexContext);
            }

            DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapReduceQueryResultRetriever(indexContext, fieldsToFetch);
        }

        private static Tree GetMapEntriesTree(Transaction tx)
        {
            // map entries structure
            // MapEntries tree has the following entries
            // -> { document key, fixed size tree }
            // each fixed size tree stored records like 
            // -> { identifier of a map result, hash of a reduce key for the map result }

            return tx.CreateTree("MapEntries");
        }

        private unsafe void PutMappedResult(BlittableJsonReaderObject mappedResult, LazyStringValue documentKey, ulong reduceKeyHash, ReduceKeyState state, TransactionOperationContext indexContext)
        {
            var documentMapEntries = _mapReduceWorkContext.MapEntries.FixedTreeFor(documentKey, sizeof(ulong));

            long id = -1;

            if (documentMapEntries.NumberOfEntries > 0)
            {
                var mapEntries = GetMapEntriesForDocument(documentKey, documentMapEntries);

                if (mapEntries.Count == 1 && mapEntries[0].ReduceKeyHash == reduceKeyHash)
                {
                    // update of existing entry, reduce key remained the same - we are going to overwrite the map result only
                    id = mapEntries[0].Id;
                }
                else
                {
                    foreach (var mapEntry in mapEntries)
                    {
                        var previousState = GetReduceKeyState(mapEntry.ReduceKeyHash, indexContext, create: false);

                        fixed (long* ptr = &mapEntry.Id)
                            previousState.Tree.Delete(Slice.External(indexContext.Allocator, (byte*)ptr, sizeof(long)));

                        documentMapEntries.Delete(mapEntry.Id);

                        _mapReduceWorkContext.EntryDeleted(mapEntry);
                    }
                }
            }

            if (id == -1)
            {
                id = _mapReduceWorkContext.GetNextIdentifier();
                documentMapEntries.Add(id, Slice.External(indexContext.Allocator, (byte*)&reduceKeyHash, sizeof(ulong)));
            }

            var pos = state.Tree.DirectAdd(Slice.External(indexContext.Allocator, (byte*)&id, sizeof(long)), mappedResult.Size);

            mappedResult.CopyTo(pos);
        }

        public static unsafe List<MapEntry> GetMapEntriesForDocument(LazyStringValue documentKey, FixedSizeTree documentMapEntries)
        {
            var entries = new List<MapEntry>((int)documentMapEntries.NumberOfEntries);

            using (var it = documentMapEntries.Iterate())
            {
                do
                {
                    var currentKey = it.CurrentKey;
                    ulong reduceKeyHash;

                    it.CreateReaderForCurrent().Read((byte*)&reduceKeyHash, sizeof(ulong));

                    entries.Add(new MapEntry
                    {
                        Id = currentKey,
                        ReduceKeyHash = reduceKeyHash
                    });
                } while (it.MoveNext());
            }

            return entries;
        }

        private ReduceKeyState GetReduceKeyState(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create)
        {
            ReduceKeyState state;
            if (_mapReduceWorkContext.StateByReduceKeyHash.TryGetValue(reduceKeyHash, out state) == false)
            {
                //TODO: Need better way to handle tree names
                Tree tree;

                if (create)
                    tree = indexContext.Transaction.InnerTransaction.CreateTree("TODO_" + reduceKeyHash);
                else
                    tree = indexContext.Transaction.InnerTransaction.ReadTree("TODO_" + reduceKeyHash);

                _mapReduceWorkContext.StateByReduceKeyHash[reduceKeyHash] = state = new ReduceKeyState(tree);
            }
            return state;
        }
    }
}