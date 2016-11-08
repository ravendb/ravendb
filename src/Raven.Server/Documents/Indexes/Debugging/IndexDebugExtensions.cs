using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public static class IndexDebugExtensions
    {
        public static IDisposable GetIdentifiersOfMappedDocuments(this Index self, string startsWith, int start, int take, out IEnumerable<LazyStringValue> docIds)
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
                    docIds = Enumerable.Empty<LazyStringValue>();
                    return scope;
                }

                TreeIterator it;
                scope.EnsureDispose(it = tree.Iterate(false));
                    
                docIds = IterateKeys(it, startsWith, start, take, indexContext);
                    
                return scope.Delay();
            }
        }

        private static IEnumerable<LazyStringValue> IterateKeys(IIterator it, string prefix, int start, int take, TransactionOperationContext context)
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
                
                var key = it.CurrentKey.ToString();

                if (--take < 0)
                    yield break;

                yield return context.GetLazyString(key);
            } while (it.MoveNext());
        }

        public static IDisposable GetReduceTree(this Index self, string docId, out IEnumerable<ReduceTreeNode> trees)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext indexContext;
                scope.EnsureDispose(self._contextPool.AllocateOperationContext(out indexContext));

                RavenTransaction tx;
                scope.EnsureDispose(tx = indexContext.OpenReadTransaction());

                var mapPhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition>.MapPhaseTreeName);

                if (mapPhaseTree == null)
                    throw new Exception("TODO arek");

                var reducePhaseTree = tx.InnerTransaction.ReadTree(MapReduceIndexBase<MapReduceIndexDefinition>.ReducePhaseTreeName);

                if (reducePhaseTree == null)
                    throw new Exception("TODO arek");

                var typePerHash = reducePhaseTree.FixedTreeFor(MapReduceIndexBase<MapReduceIndexDefinition>.ResultsStoreTypesTreeName, sizeof(byte));

                Slice docIdAsSlice;
                scope.EnsureDispose(Slice.From(indexContext.Allocator, docId, out docIdAsSlice));

                FixedSizeTree mapEntries;
                scope.EnsureDispose(mapEntries = mapPhaseTree.FixedTreeFor(docId, sizeof(long)));

                trees = IterateTrees(self, mapEntries, typePerHash, reducePhaseTree, indexContext);

                return scope.Delay();
            }
        }

        private static IEnumerable<ReduceTreeNode> IterateTrees(Index self, FixedSizeTree mapEntries, FixedSizeTree typePerHash, Tree reducePhaseTree, TransactionOperationContext indexContext)
        {
            HashSet<ulong> rendered = new HashSet<ulong>();

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
                            yield return RenderTreeNodes(store.Tree, indexContext);
                            break;
                        case MapResultsStorageType.Nested:
                            yield return RenderNestedSectionNodes(store.GetNestedResultsSection(reducePhaseTree), indexContext);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(store.Type.ToString());
                    }
                }
            }
        }

        private static unsafe ReduceTreeNode RenderTreeNodes(Tree tree, JsonOperationContext context)
        {
            var stack = new Stack<ReduceTreeNode>();
            var rootPage = tree.GetReadOnlyTreePage(tree.State.RootPageNumber);

            var rootNode = new ReduceTreeNode(rootPage)
            {
                Name = "Root of " + tree.Name
            };

            stack.Push(rootNode);

            while (stack.Count > 0)
            {
                var node = stack.Pop();
                var page = node.Page;

                if (page.NumberOfEntries == 0 && page != rootPage)
                    throw new InvalidOperationException($"The page {page.PageNumber} is empty");
                
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    if (page.IsBranch)
                    {
                        var p = page.GetNode(i)->PageNumber;

                        var childNode = new ReduceTreeNode(tree.GetReadOnlyTreePage(p));

                        node.Children.Add(childNode);

                        stack.Push(childNode);
                    }
                    else
                    {
                        var valueReader = TreeNodeHeader.Reader(tree.Llt, page.GetNode(i));

                        node.Children.Add(new ReduceTreeNode
                        {
                            Data = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, context)
                        });
                    }
                }
            }

            return rootNode;
        }

        private static ReduceTreeNode RenderNestedSectionNodes(NestedMapResultsSection section, JsonOperationContext context)
        {
            var children = new List<BlittableJsonReaderObject>();

            var count = section.GetResults(context, children);

            var rootNode = new ReduceTreeNode(count)
            {
                Name = "Result of " + section.Name
            };

            foreach (var data in children)
            {
                rootNode.Children.Add(new ReduceTreeNode()
                {
                    Data = data
                });
            }

            return rootNode;
        }
    }
}