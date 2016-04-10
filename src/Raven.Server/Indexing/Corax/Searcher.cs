using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Server.Indexing.Corax.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Indexing.Corax
{
    public class Searcher : IDisposable
    {
        private readonly FullTextIndex _parent;
        private readonly TransactionOperationContext _context;

        public Searcher(FullTextIndex parent)
        {
            _parent = parent;
            _context = new TransactionOperationContext(_parent.Pool, _parent.Env);
            _context.OpenReadTransaction();
            _context.CachedProperties.Version = 1;
        }

        public unsafe string[] Query(QueryDefinition qd)
        {
            var entries = new Table(_parent.EntriesSchema, "IndexEntries", _context.Transaction.InnerTransaction);
            //TODO: implement using heap
            //var heap = new Heap<QueryMatch>(qd.Take, QueryMatchScoreSorter.Instance);
            qd.Query.Initialize(_parent, _context, entries);
            var queryMatches = qd.Query.Execute();
            if (qd.Sort == null || qd.Sort.Length == 0)
            {
                Array.Sort(queryMatches, QueryMatchScoreSorter.Instance);
            }
            else
            {
                Array.Sort(queryMatches, new EntrySorter(_context, entries, qd.Sort));
            }

            var entryId = 0L;
            var entryKey = new Slice((byte*)&entryId, sizeof(long));
            var results = new string[Math.Min(queryMatches.Length, qd.Take)];
            for (int i = 0; i < results.Length; i++)
            {
                entryId = IPAddress.NetworkToHostOrder(queryMatches[i].DocumentId);
                var tvr = entries.ReadByKey(entryKey);
                int size;
                var entry = new BlittableJsonReaderObject(tvr.Read(1, out size), size, _context);
                entry.TryGet(Constants.DocumentIdFieldName, out results[i]);
            }

            return results;
        }

        public void Dispose()
        {
            _context?.Dispose();
        }
    }

    public unsafe class EntrySorter : IComparer<QueryMatch>
    {
        private readonly JsonOperationContext _context;
        private readonly Table _entries;
        private readonly QueryDefinition.OrderBy[] _sort;

        public EntrySorter(JsonOperationContext context, Table entries, QueryDefinition.OrderBy[] sort)
        {
            _context = context;
            _entries = entries;
            _sort = sort;
        }


        public int Compare(QueryMatch x, QueryMatch y)
        {
            var xEntryId = IPAddress.HostToNetworkOrder(x.DocumentId);
            var yEntryId = IPAddress.HostToNetworkOrder(y.DocumentId);
            var xKey = new Slice((byte*)&xEntryId, sizeof(long));
            var yKey = new Slice((byte*)&yEntryId, sizeof(long));

            var xReader = _entries.ReadByKey(xKey);
            var yReader = _entries.ReadByKey(yKey);

            int size;
            var xEntry = new BlittableJsonReaderObject(xReader.Read(1, out size), size, _context);
            var yEntry = new BlittableJsonReaderObject(yReader.Read(1, out size), size, _context);

            for (int i = 0; i < _sort.Length; i++)
            {
                var factor = _sort[i].Descending ? -1 : 1;
                object xProp;
                object yProp;
                if (xEntry.TryGetMember(_sort[i].Name, out xProp) == false)
                {
                    if (yEntry.TryGetMember(_sort[i].Name, out yProp))
                        return 1 * factor;
                    continue;// both don't have the property, ignoring
                }
                if (yEntry.TryGetMember(_sort[i].Name, out yProp) == false)
                    return -1 * factor;

                if (xProp == null && yProp == null)
                    continue;
                if (xProp == null)
                    return 1 * factor;
                if (yProp == null)
                    return -1 * factor;

                int compare;
                if (AreComparables<long>(xProp, yProp, out compare))
                    return compare * factor;
                if (AreComparables<bool>(xProp, yProp, out compare))
                    return compare * factor;
                if (AreComparables<LazyStringValue>(xProp, yProp, out compare))
                    return compare * factor;
                throw new InvalidOperationException($"Cannot sort {xProp} and {yProp}");
            }

            return x.DocumentId.CompareTo(y.DocumentId);
        }

        private static bool AreComparables<T>(object xProp, object yProp, out int compare)
            where T : IComparable<T>
        {
            if (xProp is T)
            {
                if (yProp is T)
                {
                    var compareResult = ((T)xProp).CompareTo((T)yProp);
                    if (compareResult != 0)
                    {
                        compare = compareResult;
                        return true;
                    }
                }
                else
                {
                    {
                        compare = -1;
                        return true;
                    }
                }
            }
            if (yProp is T)
            {
                compare = 1;
                return true;
            }
            compare = 0;
            return false;
        }
    }
}