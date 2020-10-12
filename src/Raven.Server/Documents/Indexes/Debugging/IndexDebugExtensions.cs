using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
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
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public static class IndexDebugExtensions
    {
        public static IDisposable GetIdentifiersOfMappedDocuments(this Index self, string startsWith, int start, int take, out IEnumerable<string> docIds)
        {
            if (self.Type.IsMapReduce() == false)
                throw new NotSupportedException("Getting doc ids for map indexes is not supported");

            using (var scope = new DisposableScope())
            {
                scope.EnsureDispose(self._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext));

                RavenTransaction tx;
                scope.EnsureDispose(tx = indexContext.OpenReadTransaction());

                var tree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition, IndexField>.MapPhaseTreeName);

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

            ByteStringContext.InternalScope? scope = null;
            try
            {
                if (string.IsNullOrEmpty(prefix) == false)
                {
                    if (SetupPrefix(it, prefix, context, out scope) == false)
                        yield break;
                }
                else if (it.Seek(MapReduceIndexingContext.LastMapResultIdKey))
                {
                    if (it.MoveNext() == false)
                        yield break;
                }

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
            finally
            {
                if (scope != null)
                    scope.Value.Dispose();
            }
        }

        private static bool SetupPrefix(IIterator it, string prefix, TransactionOperationContext context,
            out ByteStringContext.InternalScope? scope)
        {
            scope = Slice.From(context.Transaction.InnerTransaction.Allocator, prefix, out Slice prefixSlice);

            it.SetRequiredPrefix(prefixSlice);

            if (it.Seek(prefixSlice))
                return true;

            scope.Value.Dispose();
            scope = null;

            it.SetRequiredPrefix(Slices.Empty);

            if (it.Seek(Slices.BeforeAllKeys) == false)
                return false;

            if (SliceComparer.Compare(it.CurrentKey, MapReduceIndexingContext.LastMapResultIdKey) == 0)
            {
                if (it.MoveNext() == false)
                    return false;
            }
            var firstKey = it.CurrentKey.ToString();
            if (it.Seek(Slices.AfterAllKeys) == false)
                return false;
            var lastKey = it.CurrentKey.ToString();

            int index = -1;
            for (int i = 0; i < Math.Min(firstKey.Length, lastKey.Length); i++)
            {
                if (firstKey[i] != lastKey[i])
                {
                    break;
                }
                index = i;
            }
            if (index == -1)
                return false;

            prefix = firstKey.Substring(0, index + 1) + prefix;

            scope = Slice.From(context.Transaction.InnerTransaction.Allocator, prefix, out prefixSlice);

            it.SetRequiredPrefix(prefixSlice);

            if (it.Seek(prefixSlice) == false)
            {
                scope.Value.Dispose();
                scope = null;
                return false;
            }
            return true;
        }

        public static IDisposable GetReduceTree(this Index self, string[] docIds, out IEnumerable<ReduceTree> trees)
        {
            using (var scope = new DisposableScope())
            {
                scope.EnsureDispose(self._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext));

                RavenTransaction tx;
                scope.EnsureDispose(tx = indexContext.OpenReadTransaction());

                var mapPhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition, IndexField>.MapPhaseTreeName);

                if (mapPhaseTree == null)
                {
                    trees = Enumerable.Empty<ReduceTree>();
                    return scope;
                }

                var reducePhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition, IndexField>.ReducePhaseTreeName);

                if (reducePhaseTree == null)
                {
                    trees = Enumerable.Empty<ReduceTree>();
                    return scope;
                }

                var mapEntries = new List<FixedSizeTree>(docIds.Length);
                foreach (var docId in docIds)
                {
                    FixedSizeTree mapEntriesTree;
                    scope.EnsureDispose(mapEntriesTree = mapPhaseTree.FixedTreeFor(docId.ToLower(), sizeof(long)));
                    mapEntries.Add(mapEntriesTree);
                }

                FixedSizeTree typePerHash;
                scope.EnsureDispose(typePerHash = reducePhaseTree.FixedTreeFor(MapReduceIndexBase<MapReduceIndexDefinition, IndexField>.ResultsStoreTypesTreeName, sizeof(byte)));

                trees = IterateTrees(self, mapEntries, reducePhaseTree, typePerHash, indexContext, scope);

                return scope.Delay();
            }
        }

        private static IEnumerable<ReduceTree> IterateTrees(Index self, List<FixedSizeTree> mapEntries,
            Tree reducePhaseTree, FixedSizeTree typePerHash, TransactionOperationContext indexContext, DisposableScope scope)
        {
            var reduceKeys = new HashSet<ulong>();
            var idToDocIdHash = new Dictionary<long, string>();

            foreach (var tree in mapEntries)
                foreach (var mapEntry in MapReduceIndexBase<MapReduceIndexDefinition, IndexField>.GetMapEntries(tree))
                {
                    reduceKeys.Add(mapEntry.ReduceKeyHash);
                    idToDocIdHash[mapEntry.Id] = tree.Name.ToString();
                }

            foreach (var reduceKeyHash in reduceKeys)
            {
                MapReduceResultsStore store;

                var mapReduceIndex = self as MapReduceIndex;

                if (mapReduceIndex != null)
                    store = mapReduceIndex.CreateResultsStore(typePerHash,
                        reduceKeyHash, indexContext, false);
                else
                    store = ((AutoMapReduceIndex)self).CreateResultsStore(typePerHash,
                        reduceKeyHash, indexContext, false);

                using (store)
                {
                    ReduceTree tree;
                    switch (store.Type)
                    {
                        case MapResultsStorageType.Tree:
                            tree = RenderTree(store.Tree, reduceKeyHash, idToDocIdHash, self, indexContext);
                            break;
                        case MapResultsStorageType.Nested:
                            tree = RenderNestedSection(store.GetNestedResultsSection(reducePhaseTree), reduceKeyHash, idToDocIdHash, self,
                                indexContext);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(store.Type.ToString());
                    }

                    scope.EnsureDispose(tree);
                    yield return tree;
                }
            }
        }

        private static unsafe ReduceTree RenderTree(Tree tree, ulong reduceKeyHash, Dictionary<long, string> idToDocIdHash, Index index, TransactionOperationContext context)
        {
            var stack = new Stack<ReduceTreePage>();
            var rootPage = tree.GetReadOnlyTreePage(tree.State.RootPageNumber);

            var root = new ReduceTreePage(rootPage);

            root.AggregationResult = GetReduceResult(reduceKeyHash, index, context);

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

                if (page.IsCompressed)
                {
                    var decompressed = tree.DecompressPage(page, DecompressionUsage.Read, true);

                    node.DecompressedLeaf = decompressed;
                    page = decompressed;
                }

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

                        using (page.GetNodeKey(tx, i, out Slice s))
                        {
                            var mapEntryId = Bits.SwapBytes(*(long*)s.Content.Ptr);

                            if (idToDocIdHash.TryGetValue(mapEntryId, out string docId))
                                entry.Source = docId;
                        }

                        node.Entries.Add(entry);
                    }
                }

                if (node != root)
                    node.AggregationResult = GetAggregationResult(node.PageNumber, table, context);
            }

            return new ReduceTree
            {
                DisplayName = GetTreeName(root.AggregationResult, index.Definition, context),
                Name = tree.Name.ToString(),
                Root = root,
                Depth = tree.State.Depth,
                PageCount = tree.State.PageCount,
                NumberOfEntries = tree.State.NumberOfEntries
            };
        }

        private static string GetTreeName(BlittableJsonReaderObject reduceEntry, IndexDefinitionBase indexDefinition, JsonOperationContext context)
        {
            HashSet<CompiledIndexField> groupByFields;

            if (indexDefinition is MapReduceIndexDefinition)
                groupByFields = ((MapReduceIndexDefinition)indexDefinition).GroupByFields;
            else if (indexDefinition is AutoMapReduceIndexDefinition)
                groupByFields = ((AutoMapReduceIndexDefinition)indexDefinition).GroupByFields.Keys
                    .Select(x => (CompiledIndexField)new SimpleField(x))
                    .ToHashSet();
            else
                throw new InvalidOperationException("Invalid map reduce index definition: " + indexDefinition.GetType());

            foreach (var prop in reduceEntry.GetPropertyNames())
            {
                var skip = false;
                foreach (var groupByField in groupByFields)
                {
                    if (groupByField.IsMatch(prop))
                    {
                        skip = true;
                        break;
                    }
                }

                if (skip)
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

            using (Slice.External(context.Allocator, (byte*)&tmp, sizeof(long), out Slice pageNumberSlice))
            {
                table.ReadByKey(pageNumberSlice, out TableValueReader tvr);

                var numberOfResults = *(int*)tvr.Read(ReduceMapResultsOfStaticIndex.NumberOfResultsPosition, out int _);

                if (numberOfResults == 0)
                    return context.ReadObject(new DynamicJsonValue(), "debug-reduce-result");

                return new BlittableJsonReaderObject(tvr.Read(3, out int size), size, context);
            }
        }

        private static ReduceTree RenderNestedSection(NestedMapResultsSection section, ulong reduceKeyHash, Dictionary<long, string> idToDocIdHash, Index index, TransactionOperationContext context)
        {
            var entries = new Dictionary<long, BlittableJsonReaderObject>();

            var root = new ReduceTreePage(section.RelevantPage);

            root.AggregationResult = GetReduceResult(reduceKeyHash, index, context);

            section.GetResultsForDebug(context, entries);

            foreach (var item in entries)
            {
                var entry = new MapResultInLeaf
                {
                    Data = item.Value
                };

                var id = Bits.SwapBytes(item.Key);

                if (idToDocIdHash.TryGetValue(id, out string docId))
                    entry.Source = docId;

                root.Entries.Add(entry);
            }

            return new ReduceTree
            {
                DisplayName = GetTreeName(root.AggregationResult, index.Definition, context),
                Name = section.Name.ToString(),
                Root = root,
                Depth = 1,
                PageCount = 1,
                NumberOfEntries = entries.Count
            };
        }

        private static BlittableJsonReaderObject GetReduceResult(ulong reduceKeyHash, Index index, TransactionOperationContext context)
        {
            using (var reader = index.IndexPersistence.OpenIndexReader(context.Transaction.InnerTransaction))
            using (index.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var queryParameters = context.ReadObject(new DynamicJsonValue
                {
                    ["p0"] = reduceKeyHash.ToString()
                }, "query/parameters");
                var query = new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE '{Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName}' = $p0", queryParameters);

                var fieldsToFetch = new FieldsToFetch(query, index.Definition);

                var retriever = new MapReduceQueryResultRetriever(null, null, null, null, context, fieldsToFetch, null);
                var result = reader
                     .Query(query, null, fieldsToFetch, new Reference<int>(), new Reference<int>(), retriever, ctx, null, CancellationToken.None)
                    .ToList();

                if (result.Count == 0)
                    return context.ReadObject(new DynamicJsonValue(), "debug-reduce-result");

                if (result.Count > 1)
                    throw new InvalidOperationException("Cannot have multiple reduce results for a single reduce key");

                return result[0].Result.Data;
            }
        }
    }
}
