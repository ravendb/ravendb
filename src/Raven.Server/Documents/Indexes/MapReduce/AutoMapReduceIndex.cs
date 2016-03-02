using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
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
                    using (indexContext.OpenReadTransaction())
                    {
                        lastMappedEtag = _parent.ReadLastMappedEtag(indexContext.Transaction, collection);
                    }

                    _cancellationToken.ThrowIfCancellationRequested();

                    var lastEtag = DoMap(collection, lastMappedEtag);

                    var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;
                    var parentPagesToAggregate = new HashSet<long>();
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
                                parentPagesToAggregate.Add(parentPage);

                            AggregateLeafPage(page, lowLevelTransaction, modifiedPage);
                        }

                        long tmp = 0;
                        Slice pageNumberSlice = new Slice((byte*)&tmp, sizeof(long));
                        foreach (var freedPage in modifiedState.FreedPages)
                        {
                            tmp = freedPage;
                            _table.DeleteByKey(pageNumberSlice);
                        }

                        HashSet<long> other = new HashSet<long>();
                        while (parentPagesToAggregate.Count > 0)
                        {
                            // if we have deep hierarchy, only allocate two sets
                            HashSet<long> swap = other;
                            other = parentPagesToAggregate;
                            parentPagesToAggregate = swap;
                            parentPagesToAggregate.Clear();

                            foreach (var parentPage in other)
                            {
                                var page = lowLevelTransaction.GetPage(parentPage).ToTreePage();
                                if (page.IsBranch == false)
                                {
                                    //TODO: this is an error
                                    throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " + page.PageNumber);
                                }



                                for (int i = 0; i < page.NumberOfEntries; i++)
                                {
                                    var pageNumber = page.GetNode(i)->PageNumber;
                                    var tvr = _table.ReadByKey(new Slice((byte*)&pageNumber, sizeof(long)));
                                    if (tvr == null)
                                    {
                                        //TODO: this is an error
                                        throw new InvalidOperationException(
                                            "Couldn't find pre-computed results for existing page " + pageNumber);
                                    }
                                    int size;
                                    _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(1, out size), size,
                                        indexContext));
                                }
                                AggregateBatchResults(parentPage);
                            }
                        }
                    }

                    if (_count == 0)
                        return;

                    if (lastEtag <= lastMappedEtag)
                        return;

                    //using (var tx = indexContext.OpenWriteTransaction())
                    //{
                    //    WriteLastMappedEtag(tx, collection, lastEtag);

                    //    tx.Commit();
                    //}

                    _parent._mre.Set(); // might be more
                }
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
                            mappedResult[indexField.Name] = result;
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
                            reduceHashKey = Hashing.XXHash64.Calculate(reduceKeyObject.BasePointer,
                                reduceKeyObject.Size);
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
                            //TODO: use etags as the key?
                            var pos = state.Tree.DirectAdd(new Slice(document.Key.Buffer, (ushort)document.Key.Size),
                                mappedresult.Size);
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

            private void AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, long modifiedPage)
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                    var reduceEntry = new BlittableJsonReaderObject(valueReader.Base,
                        valueReader.Length, indexContext);
                    _aggregationBatch.Add(reduceEntry);
                }

                AggregateBatchResults(modifiedPage);

            }

            private void AggregateBatchResults(long modifiedPage)
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
                using (var resultObj = indexContext.ReadObject(djv, "map/reduce"))
                {
                    _table.Set(new TableValueBuilder
                    {
                        {(byte*) &modifiedPage, sizeof (long)}, // page number
                        {resultObj.BasePointer, resultObj.Size}
                    });
                }

            }


            public void Dispose()
            {
                databaseContext?.Dispose();
                indexContext?.Dispose();
            }
        }

        protected override void DoIndexingWork(CancellationToken cancellationToken)
        {
            using (var instance = new ReducingExecuter(this, cancellationToken))
                instance.Execute();
        }

    }
}