using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public static class IndexDebugExtensions
    {
        public static IDisposable GetIdentifiersOfMappedDocuments(this Index self, string startsWith, int start, int take, out IEnumerable<string> docIds)
        {
            if (self.Type.IsMapReduce() == false)
                throw new NotSupportedException("Getting doc ids for map indexes is not supported");

            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext indexContext;
                scope.EnsureDispose(self._contextPool.AllocateOperationContext(out indexContext));

                RavenTransaction tx;
                scope.EnsureDispose(tx = indexContext.OpenReadTransaction());

                var tree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition>.MapPhaseTreeName);

                if (tree == null)
                {
                    docIds = Enumerable.Empty<string>();
                    return scope;
                }

                TreeIterator it;
                scope.EnsureDispose(it = tree.Iterate(false));
                    
                docIds = IterateKeys(it, startsWith, start, take, indexContext);
                    
                return scope.Delay();
            }
        }

        private static IEnumerable<string> IterateKeys(IIterator it, string prefix, int start, int take, TransactionOperationContext context)
        {
            if (it.Seek(Slices.BeforeAllKeys) == false)
                yield break;

            if (string.IsNullOrEmpty(prefix) == false)
            {
                Slice prefixSlice;
                Slice.From(context.Transaction.InnerTransaction.Allocator, prefix, out prefixSlice);

                it.RequiredPrefix = prefixSlice;
                
                if (it.Seek(prefixSlice) == false)
                    yield break;
            }
            else if (it.Seek(MapReduceIndexingContext.LastMapResultIdKey))
                it.MoveNext();

            do
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
 
                if (--take < 0)
                    yield break;

                yield return it.CurrentKey.ToString();
            } while (it.MoveNext());
        }

        public static IDisposable GetReduceTree(this Index self, string docId, out IEnumerable<ReduceTree> trees)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext indexContext;
                scope.EnsureDispose(self._contextPool.AllocateOperationContext(out indexContext));

                RavenTransaction tx;
                scope.EnsureDispose(tx = indexContext.OpenReadTransaction());

                var mapPhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition>.MapPhaseTreeName);

                if (mapPhaseTree == null)
                {
                    trees = Enumerable.Empty<ReduceTree>();
                    return scope;
                }

                var reducePhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition>.ReducePhaseTreeName);

                if (reducePhaseTree == null)
                {
                    trees = Enumerable.Empty<ReduceTree>();
                    return scope;
                }

                Slice docIdAsSlice;
                scope.EnsureDispose(Slice.From(indexContext.Allocator, docId, out docIdAsSlice));

                FixedSizeTree mapEntries;
                scope.EnsureDispose(mapEntries = mapPhaseTree.FixedTreeFor(docId, sizeof(long)));

                FixedSizeTree typePerHash;
                scope.EnsureDispose(typePerHash = reducePhaseTree.FixedTreeFor(MapReduceIndexBase<MapReduceIndexDefinition>.ResultsStoreTypesTreeName, sizeof(byte)));

                trees = IterateTrees(self, mapEntries, reducePhaseTree, typePerHash, indexContext);

                return scope.Delay();
            }
        }

        private static IEnumerable<ReduceTree> IterateTrees(Index self, FixedSizeTree mapEntries,
            Tree reducePhaseTree, FixedSizeTree typePerHash, TransactionOperationContext indexContext)
        {
            var rendered = new HashSet<ulong>();

            foreach (var mapEntry in MapReduceIndexBase<MapReduceIndexDefinition>.GetMapEntries(mapEntries))
            {
                if (rendered.Add(mapEntry.ReduceKeyHash) == false)
                    continue;

                MapReduceResultsStore store;

                var mapReduceIndex = self as MapReduceIndex;

                if (mapReduceIndex != null)
                    store = mapReduceIndex.CreateResultsStore(typePerHash,
                        mapEntry.ReduceKeyHash, indexContext, false);
                else
                    store = ((AutoMapReduceIndex) self).CreateResultsStore(typePerHash,
                        mapEntry.ReduceKeyHash, indexContext, false);

                using (store)
                {
                    switch (store.Type)
                    {
                        case MapResultsStorageType.Tree:
                            yield return RenderTree(store.Tree, mapEntry, mapEntries.Name.ToString(), self, indexContext);
                            break;
                        case MapResultsStorageType.Nested:
                            yield return
                                RenderNestedSection(store.GetNestedResultsSection(reducePhaseTree), mapEntry, mapEntries.Name.ToString(), self,
                                    indexContext);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(store.Type.ToString());
                    }
                }
            }
        }

        private static unsafe ReduceTree RenderTree(Tree tree, MapEntry mapEntry, string sourceDocId, Index index, TransactionOperationContext context)
        {
            var stack = new Stack<ReduceTreePage>();
            var rootPage = tree.GetReadOnlyTreePage(tree.State.RootPageNumber);

            var root = new ReduceTreePage(rootPage);

            root.AggregationResult = GetReduceResult(mapEntry.ReduceKeyHash, index, context);
            
            stack.Push(root);

            var table =
                context.Transaction.InnerTransaction.OpenTable(
                    ReduceMapResultsBase<MapReduceIndexDefinition>.ReduceResultsSchema,
                    ReduceMapResultsBase<MapReduceIndexDefinition>.PageNumberToReduceResultTableName);

            var tx = tree.Llt;
            while (stack.Count > 0)
            {
                var node = stack.Pop();
                var page = node.Page;

                if (page.NumberOfEntries == 0 && page != rootPage)
                    throw new InvalidOperationException($"The page {page.PageNumber} is empty");
                
                for (var i = 0; i < page.NumberOfEntries; i++)
                {
                    if (page.IsBranch)
                    {
                        var p = page.GetNode(i)->PageNumber;

                        var childNode = new ReduceTreePage(tree.GetReadOnlyTreePage(p));

                        node.Children.Add(childNode);

                        stack.Push(childNode);
                    }
                    else
                    {
                        var entry = new MapResultInLeaf();

                        var valueReader = TreeNodeHeader.Reader(tx, page.GetNode(i));
                        entry.Data = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, context);

                        Slice s;
                        using (page.GetNodeKey(tx, i, out s))
                        {
                            var mapEntryId = *(long*) s.Content.Ptr;
                            if (mapEntryId == mapEntry.Id)
                            {
                                entry.Source = sourceDocId;
                            }
                        }
                        
                        node.Entries.Add(entry);
                    }
                }

                if (node != root)
                    node.AggregationResult = GetAggregationResult(node.PageNumber, table, context);
            }
            
            return new ReduceTree
            {
                Name = GetTreeName(root.AggregationResult, index.Definition, context),
                Root = root,
                Depth = tree.State.Depth,
                PageCount = tree.State.PageCount,
                NumberOfEntries = tree.State.NumberOfEntries
            };
        }

        private static string GetTreeName(BlittableJsonReaderObject reduceEntry, IndexDefinitionBase indexDefinition, TransactionOperationContext context)
        {
            HashSet<string> groupByFields;

            if (indexDefinition is MapReduceIndexDefinition)
                groupByFields = ((MapReduceIndexDefinition) indexDefinition).GroupByFields;
            else if (indexDefinition is AutoMapReduceIndexDefinition)
                groupByFields = ((AutoMapReduceIndexDefinition)indexDefinition).GroupByFields.Keys.ToHashSet();
            else
                throw new InvalidOperationException("Invalid map reduce index definition: " + indexDefinition.GetType());

            foreach (var prop in reduceEntry.GetPropertyNames())
            {
                if (groupByFields.Contains(prop))
                    continue;

                if (reduceEntry.Modifications == null)
                    reduceEntry.Modifications = new DynamicJsonValue(reduceEntry);  

                reduceEntry.Modifications.Remove(prop);
            }

            var reduceKey = context.ReadObject(reduceEntry, "debug: creating reduce tree name");

            return reduceKey.ToString();
        }

        private static unsafe BlittableJsonReaderObject GetAggregationResult(long pageNumber, Table table, TransactionOperationContext context)
        {
            var tmp = Bits.SwapBytes(pageNumber);

            Slice pageNumberSlice;
            using (Slice.External(context.Allocator, (byte*)&tmp, sizeof(long), out pageNumberSlice))
            {
                var tvr = table.ReadByKey(pageNumberSlice);

                int size;
                return new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);
            }
        }

        private static ReduceTree RenderNestedSection(NestedMapResultsSection section, MapEntry mapEntry, string sourceDocId, Index index, TransactionOperationContext context)
        {
            var entries = new Dictionary<long, BlittableJsonReaderObject>();

            var root = new ReduceTreePage(section.RelevantPage);

            root.AggregationResult = GetReduceResult(mapEntry.ReduceKeyHash, index, context);

            section.GetResultsForDebug(context, entries);

            foreach (var item in entries)
            {
                var entry = new MapResultInLeaf
                {
                    Data = item.Value
                };

                if (item.Key == mapEntry.Id)
                    entry.Source = sourceDocId;

                root.Entries.Add(entry);
            }

            return new ReduceTree
            {
                Name = GetTreeName(root.AggregationResult, index.Definition, context),
                Root = root,
                Depth = 1,
                PageCount = 1,
                NumberOfEntries = entries.Count
            };
        }

        private static BlittableJsonReaderObject GetReduceResult(ulong reduceKeyHash, Index index, TransactionOperationContext context)
        {
            using (var reader = index.IndexPersistence.OpenIndexReader(context.Transaction.InnerTransaction))
            {
                var query = new IndexQueryServerSide
                {
                    Query = $"{Constants.Indexing.Fields.ReduceKeyFieldName}:{reduceKeyHash}"
                };

                var fieldsToFetch = new FieldsToFetch(query, index.Definition, null);

                var result = reader.Query(query, fieldsToFetch, new Reference<int>(), new Reference<int>(),
                    new MapReduceQueryResultRetriever(context, fieldsToFetch), CancellationToken.None).ToList();

                if (result.Count != 1)
                    throw new InvalidOperationException("Cannot have multiple reduce results for a single reduce key");

                return result[0].Data;
            }
        }
    }
}