using System;
using System.Collections.Generic;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract class MapReduceIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        protected readonly MapReduceIndexingContext _mapReduceWorkContext = new MapReduceIndexingContext();

        protected MapReduceIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            _mapReduceWorkContext.MapEntries = GetMapEntriesTree(indexContext.Transaction.InnerTransaction);

            return _mapReduceWorkContext;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var documentMapEntries = _mapReduceWorkContext.MapEntries.FixedTreeFor(tombstone.Key, sizeof(ulong));

            if (documentMapEntries.NumberOfEntries == 0)
                return;

            foreach (var mapEntry in GetMapEntries(documentMapEntries))
            {
                var state = GetReduceKeyState(mapEntry.ReduceKeyHash, indexContext, create: false);

                var entryId = mapEntry.Id;
                state.Tree.Delete(Slice.External(indexContext.Allocator, (byte*)&entryId, sizeof(long)));

                _mapReduceWorkContext.EntryDeleted(mapEntry.Id);
            }

            _mapReduceWorkContext.MapEntries.DeleteFixedTreeFor(tombstone.Key, sizeof(ulong));
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

        protected unsafe void PutMapResults(LazyStringValue documentKey, IEnumerable<MapResult> mappedResults, TransactionOperationContext indexContext)
        {
            var documentMapEntries = _mapReduceWorkContext.MapEntries.FixedTreeFor(documentKey, sizeof(ulong));

            Dictionary<ulong, Queue<long>> existingIdsPerReduceKey = null;

            if (documentMapEntries.NumberOfEntries > 0)
            {
                existingIdsPerReduceKey = new Dictionary<ulong, Queue<long>>();

                var mapEntries = GetMapEntries(documentMapEntries);

                foreach (var mapEntry in mapEntries)
                {
                    Queue<long> ids;
                    if (existingIdsPerReduceKey.TryGetValue(mapEntry.ReduceKeyHash, out ids) == false)
                    {
                        ids = new Queue<long>();
                        existingIdsPerReduceKey[mapEntry.ReduceKeyHash] = ids;
                    }

                    ids.Enqueue(mapEntry.Id);
                }
            }

            foreach (var mapResult in mappedResults)
            {
                var reduceKeyHash = mapResult.ReduceKeyHash;

                long id;

                Queue<long> availableIds;
                if (existingIdsPerReduceKey != null && existingIdsPerReduceKey.TryGetValue(reduceKeyHash, out availableIds))
                {
                    // reuse id of an old entry
                    id = availableIds.Dequeue();

                    if (availableIds.Count == 0)
                        existingIdsPerReduceKey.Remove(reduceKeyHash);
                }
                else
                {
                    id = _mapReduceWorkContext.GetNextIdentifier();

                    documentMapEntries.Add(id, Slice.External(indexContext.Allocator, (byte*)&reduceKeyHash, sizeof(ulong)));
                }

                var pos = mapResult.State.Tree.DirectAdd(Slice.External(indexContext.Allocator, (byte*)&id, sizeof(long)), mapResult.Data.Size);

                mapResult.Data.CopyTo(pos);
            }

            if (existingIdsPerReduceKey != null && existingIdsPerReduceKey.Count > 0)
            {
                // need to remove remaining old entries

                foreach (var stillExisting in existingIdsPerReduceKey)
                {
                    var reduceKeyHash = stillExisting.Key;
                    var ids = stillExisting.Value;

                    var oldState = GetReduceKeyState(reduceKeyHash, indexContext, create: false);

                    while (ids.Count > 0)
                    {
                        var idToDelete = ids.Dequeue();

                        oldState.Tree.Delete(Slice.External(indexContext.Allocator, (byte*)&idToDelete, sizeof(long)));

                        documentMapEntries.Delete(idToDelete);

                        _mapReduceWorkContext.EntryDeleted(idToDelete);
                    }
                }
            }
        }

        private static unsafe List<MapEntry> GetMapEntries(FixedSizeTree documentMapEntries)
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

        public ReduceKeyState GetReduceKeyState(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create)
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