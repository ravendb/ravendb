using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public static class IndexDebugExtensions
    {
        public static IDisposable GetIdentifiersOfMappedDocuments(this Index self, string startsWith, int start, int take, out IEnumerable<LazyStringValue> docIds)
        {
            if (self.Type.IsMapReduce() == false)
                throw new NotImplementedException("Getting doc ids for map indexes is not supported");

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
    }
}