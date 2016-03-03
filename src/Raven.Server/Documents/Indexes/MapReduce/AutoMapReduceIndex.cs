using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoMapReduceIndexDefinition>
    {
        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private AutoMapReduceIndex(int indexId, AutoMapReduceIndexDefinition definition)
            : base(indexId, IndexType.Auto, definition)
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
            //var definition = AutoIndexDefinition.Load(environment);
            //var instance = new AutoMapReduceIndex(indexId, definition);
            //instance.Initialize(environment, documentDatabase);

            //return instance;
        }

        private class ReduceKeyState
        {
            public Tree Tree;
            public HashSet<long> ModifiedPages = new HashSet<long>();
            public HashSet<long> FreedPages = new HashSet<long>();
            public ReduceKeyState(Tree tree)
            {
                Tree = tree;
                Tree.PageModified += page => ModifiedPages.Add(page);
                Tree.PageFreed += page => FreedPages.Add(page);
            }
        }

        TableSchema _reduceResultsSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                Name = "PageNumber",
                StartIndex = 0,
                Count = 1
            });

        public unsafe class ReducingExecuter : IDisposable
        {
            private CancellationToken _cancellationToken;
            private readonly AutoMapReduceIndex _parent;
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;
            Dictionary<ulong, ReduceKeyState> stateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
            private Table _table;
            private int _pageSize;
            private int _count;
            private TimeSpan _docProcessingTimeout;
            List<BlittableJsonReaderObject> _aggregationBatch = new List<BlittableJsonReaderObject>();

            public ReducingExecuter(AutoMapReduceIndex parent, CancellationToken cancellationToken)
            {
                _cancellationToken = cancellationToken;
                try
                {
                    _parent = parent;
                    _parent.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext);
                    _parent._contextPool.AllocateOperationContext(out indexContext);
                    indexContext.OpenWriteTransaction();

                    _parent._reduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, "PageNumberToReduceResult");
                    _table = new Table(_parent._reduceResultsSchema, "PageNumberToReduceResult", indexContext.Transaction.InnerTransaction);
                    _pageSize = _parent.DocumentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;
                    _docProcessingTimeout = _parent.DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan;

                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }

            public void Execute()
            {
                foreach (var collection in _parent.Collections)
                {
                    long lastMappedEtag;
                    lastMappedEtag = _parent.ReadLastMappedEtag(indexContext.Transaction, collection);

                    _cancellationToken.ThrowIfCancellationRequested();

                    var lastEtag = DoMap(collection, lastMappedEtag);
                    WriteLastMappedEtag(indexContext.Transaction, collection, lastEtag);
                }

                var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;
                var parentPagesToAggregate = new Dictionary<long, Tree>();

                using (var indexWriteTx = indexContext.OpenWriteTransaction())
                using (var writer = _parent.IndexPersistence.OpenIndexWriter(indexWriteTx.InnerTransaction))
                {
                    foreach (var modifiedState in stateByReduceKeyHash.Values)
                    {
                        foreach (var modifiedPage in modifiedState.ModifiedPages)
                        {
                            if (modifiedState.FreedPages.Contains(modifiedPage))
                                continue;

                            var page = lowLevelTransaction.GetPage(modifiedPage).ToTreePage();
                            if (page.IsLeaf == false)
                                continue;

                            var parentPage = modifiedState.Tree.GetParentPageOf(page);
                            if (parentPage != -1)
                                parentPagesToAggregate[parentPage] = modifiedState.Tree;

                            using (var result = AggregateLeafPage(page, lowLevelTransaction, modifiedPage))
                            {
                                if (parentPage == -1)
                                {
                                    // write to index
                                    writer.IndexDocument(new Document()
                                    {
                                        Data = result,
                                    });
                                }
                            }
                        }

                        long tmp = 0;
                        Slice pageNumberSlice = new Slice((byte*)&tmp, sizeof(long));
                        foreach (var freedPage in modifiedState.FreedPages)
                        {
                            tmp = freedPage;
                            _table.DeleteByKey(pageNumberSlice);
                        }

                        while (parentPagesToAggregate.Count > 0)
                        {
                            var other = parentPagesToAggregate;
                            parentPagesToAggregate = new Dictionary<long, Tree>();
                            foreach (var kvp in other)
                            {
                                var pageNumber = kvp.Key;
                                var tree = kvp.Value;
                                var page = lowLevelTransaction.GetPage(pageNumber).ToTreePage();
                                if (page.IsBranch == false)
                                {
                                    //TODO: this is an error
                                    throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " + page.PageNumber);
                                }

                                var parentPage = tree.GetParentPageOf(page);
                                if (parentPage != -1)
                                    parentPagesToAggregate[parentPage] = tree;

                                for (int i = 0; i < page.NumberOfEntries; i++)
                                {
                                    var childPageNumber = page.GetNode(i)->PageNumber;
                                    var tvr = _table.ReadByKey(new Slice((byte*)&childPageNumber, sizeof(long)));
                                    if (tvr == null)
                                    {
                                        //TODO: this is an error
                                        throw new InvalidOperationException(
                                            "Couldn't find pre-computed results for existing page " + childPageNumber);
                                    }
                                    int size;
                                    _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(1, out size), size,
                                        indexContext));
                                }
                                using (var result = AggregateBatchResults(pageNumber))
                                {
                                    if (parentPage == -1)
                                    {
                                        //write to index                             
                                        writer.IndexDocument(new Document()
                                        {
                                            Data = result
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                if (_count == 0)
                    return;

                _parent._mre.Set(); // might be more
            }

            private long DoMap(string collection, long lastEtag)
            {
                using (databaseContext.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var documentsStorage = _parent.DocumentDatabase.DocumentsStorage;
                    foreach (var document in documentsStorage.GetDocumentsAfter(databaseContext, collection,
                        lastEtag + 1, 0, _pageSize))
                    {
                        _cancellationToken.ThrowIfCancellationRequested();

                        _count++;
                        lastEtag = document.Etag;

                        var mappedResult = new DynamicJsonValue();
                        var reduceKey = new DynamicJsonValue();
                        foreach (var indexField in _parent.Definition.MapFields)
                        {
                            object result;
                            _parent._blittableTraverser.TryRead(document.Data, indexField.Name, out result);
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
                        foreach (var indexField in _parent.Definition.GroupByFields)
                        {
                            object result;
                            _parent._blittableTraverser.TryRead(document.Data, indexField.Name, out result);
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
                            var pos = state.Tree.DirectAdd(new Slice(document.Key.Buffer, (ushort)document.Key.Size), mappedresult.Size);
                            mappedresult.CopyTo(pos);
                        }

                        if (sw.Elapsed > _docProcessingTimeout)
                        {
                            break;
                        }
                    }
                }
                return lastEtag;
            }

            private BlittableJsonReaderObject AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, long modifiedPage)
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                    var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, indexContext);
                    _aggregationBatch.Add(reduceEntry);
                }

                return AggregateBatchResults(modifiedPage);
            }

            private BlittableJsonReaderObject AggregateBatchResults(long modifiedPage)
            {
                int sum = 0;
                foreach (var obj in _aggregationBatch)
                {
                    int cur;
                    if (obj.TryGet("Count", out cur))
                        sum += cur;
                }
                _aggregationBatch.Clear();
                var djv = new DynamicJsonValue
                {
                    ["Count"] = sum
                };
                var resultObj = indexContext.ReadObject(djv, "map/reduce");
                _table.Set(new TableValueBuilder
                {
                    {(byte*) &modifiedPage, sizeof (long)}, // page number
                    {resultObj.BasePointer, resultObj.Size}
                });

                return resultObj;
            }


            public void Dispose()
            {
                databaseContext?.Dispose();
                indexContext?.Dispose();
            }
        }

        public override void DoIndexingWork(CancellationToken cancellationToken)
        {
            using (var instance = new ReducingExecuter(this, cancellationToken))
                instance.Execute();
        }
    }
}