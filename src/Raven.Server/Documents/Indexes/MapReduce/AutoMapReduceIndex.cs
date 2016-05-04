using System;
using System.Collections.Generic;
using System.Linq;
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
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoMapReduceIndexDefinition>
    {
        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private readonly TableSchema _mapResultsSchema = new TableSchema();

        private readonly MapReduceIndexingContext _mapReduceWorkContext = new MapReduceIndexingContext();

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.AutoMapReduce, definition)
        {
            _mapResultsSchema.DefineIndex("DocumentKeys", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
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
            _mapReduceWorkContext.MapEntriesTable = GetMapEntriesTable(indexContext.Transaction.InnerTransaction);

            return _mapReduceWorkContext;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var mapEntry = GetMapEntryForDocument(_mapReduceWorkContext.MapEntriesTable, tombstone.Key);

            if (mapEntry == null)
                return;

            var state = GetReduceKeyState(mapEntry.ReduceKeyHash, indexContext, create: false);

            var storageId = mapEntry.StorageId;
 
            state.Tree.Delete(new Slice((byte*)&storageId, sizeof(long)));
            _mapReduceWorkContext.MapEntriesTable.Delete(mapEntry.StorageId);
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

            var state = GetReduceKeyState(reduceHashKey, indexContext, create: true);

            using (var mappedresult = indexContext.ReadObject(mappedResult, document.Key))
            {
                PutMappedResult(mappedresult, document.Key, reduceHashKey, state, _mapReduceWorkContext.MapEntriesTable, indexContext);
            }

            DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapReduceQueryResultRetriever(indexContext, fieldsToFetch);
        }

        private Table GetMapEntriesTable(Transaction tx)
        {
            _mapResultsSchema.Create(tx, "MapResults");
            var table = new Table(_mapResultsSchema, "MapResults", tx);

            return table;
        }

        private unsafe void PutMappedResult(BlittableJsonReaderObject mappedResult, LazyStringValue documentKey, ulong reduceKeyHash, ReduceKeyState state, Table mapEntriesTable, TransactionOperationContext indexContext)
        {
            var tvb = new TableValueBuilder
            {
                { documentKey.Buffer, documentKey.Size },
                { (byte*) &reduceKeyHash, sizeof(ulong) }
            };

            var existingEntry = GetMapEntryForDocument(mapEntriesTable, documentKey);

            long storageId;
            if (existingEntry == null)
                storageId = mapEntriesTable.Insert(tvb);
            else if (existingEntry.ReduceKeyHash == reduceKeyHash)
            {
                // no need to update record in table since we have the same entry already stored
                storageId = existingEntry.StorageId;
            }
            else
            {
                var previousState = GetReduceKeyState(existingEntry.ReduceKeyHash, indexContext, create: false);

                storageId = existingEntry.StorageId;

                previousState.Tree.Delete(new Slice((byte*)&storageId, sizeof(long)));

                storageId = mapEntriesTable.Update(existingEntry.StorageId, tvb);
            }

            var pos = state.Tree.DirectAdd(new Slice((byte*)&storageId, sizeof(long)), mappedResult.Size);

            mappedResult.CopyTo(pos);
        }

        public unsafe MapEntry GetMapEntryForDocument(Table table, LazyStringValue documentKey)
        {
            var documentKeySlice = new Slice(documentKey.Buffer, (ushort) documentKey.Size);

            var seek = table.SeekForwardFrom(_mapResultsSchema.Indexes["DocumentKeys"], documentKeySlice).FirstOrDefault();

            if (seek?.Key.Compare(documentKeySlice) != 0)
                return null;

            var tvr = seek.Results.Single();

            int _;
            var ptr = tvr.Read(1, out _);

            return new MapEntry
            {
                ReduceKeyHash = *(ulong*)ptr,
                StorageId = tvr.Id
            };
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