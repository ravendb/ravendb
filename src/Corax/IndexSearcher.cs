using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Newtonsoft.Json;
using Voron.Debugging;

namespace Corax
{
    public class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment)
        {
            _environment = environment;
            _transaction = environment.ReadTransaction();
        }

        public IEnumerable<string> Query(JsonOperationContext context, QueryOp q, int take, string sort)
        {
            Table entries = _transaction.OpenTable(IndexWriter.IndexEntriesSchema, IndexWriter.IndexEntriesSlice);

            if (take < 1024)  // query "planner"
            {
                return FilterByOrder(context, q, take, sort, entries);
            }

            return SearchThenSort(context, take, sort, q, entries);
        }

        public IEnumerable<string> QueryExact(JsonOperationContext context, QueryOp q, int take = 1, string sort = null)
        {
            Table entries = _transaction.OpenTable(IndexWriter.IndexEntriesSchema, IndexWriter.IndexEntriesSlice);

            if (take <= 1)
            {
                return SearchExactSingle(context, q, entries);
            }
            
            if (take < 1024)  // query "planner"
            {
                return FilterByOrder(context, q, take, sort, entries);
            }

            return SearchThenSort(context, take, sort, q, entries);
        }

        private IEnumerable<string> SearchExactSingle(JsonOperationContext context, QueryOp q, Table entries)
        {
            var results = new Bitmap();
            q.Apply(_transaction, results, BitmapOp.Or);

            if( results.Count == 0 )
                return Enumerable.Empty<string>();

            return new[] { ExtractDocumentId(entries, results.First()) };
        }

        private IEnumerable<string> FilterByOrder(JsonOperationContext context, QueryOp q, int take, string sort, Table entries)
        {
            Tree sortTree = _transaction.ReadTree(sort);
            if (sortTree == null)
                return Enumerable.Empty<string>();

            using var it = sortTree.Iterate(false);
            if (it.Seek(Slices.BeforeAllKeys) == false)
                return Enumerable.Empty<string>();
            
            var list = new List<string>(take);
            do
            {
                FixedSizeTree fst = sortTree.FixedTreeFor(it.CurrentKey);
                using var fstIt = fst.Iterate();
                if (fstIt.Seek(0) == false)
                    continue;
                do
                {
                    long entryId = fstIt.CurrentKey;
                    var bjro = GetBlittable(context, entries, entryId);
                    if (q.IsMatch(bjro))
                    {
                        list.Add(ExtractDocumentId(entries, entryId));
                        if (list.Count == take)
                            return list;
                    }
                } while (fstIt.MoveNext());
            } while (it.MoveNext());

            return list;
        }

        private IEnumerable<string> SearchThenSort(JsonOperationContext context, int take, string sort, QueryOp q, Table entries)
        {
            var results = new Bitmap();
            q.Apply(_transaction, results, BitmapOp.Or);

            var heap = new SortedList<string, string>();

            foreach (var entryId in results)
            {
                var id = ExtractDocumentId(entries, entryId);
                string sortField = ExtractField(entryId, context, entries, sort);
                if (heap.Count < take)
                {
                    heap.Add(sortField, id);
                }
                else if (string.Compare(heap.Keys[heap.Count - 1], sortField, StringComparison.Ordinal) > 0)
                {
                    heap.RemoveAt(heap.Count - 1);
                    heap.Add(sortField, id);
                }
            }

            return heap.Values;
        }

        private string ExtractField(long entryId, JsonOperationContext context, Table entries, string sort)
        {
            var bjro = GetBlittable(context, entries, entryId);

            return bjro[sort].ToString();
        }

        unsafe string ExtractDocumentId(Table entries, long entryId)
        {
            entries.DirectRead(entryId, out TableValueReader tvr);

            byte* ptr = tvr.Read((int) IndexWriter.IndexEntriesTable.DocumentId, out var size);

            var str = Encoding.UTF8.GetString(ptr, size);
            return str;
        }

        private static unsafe BlittableJsonReaderObject GetBlittable(JsonOperationContext context, Table entries, long entryId)
        {
            entries.DirectRead(entryId, out TableValueReader tvr);

            byte* ptr = tvr.Read((int) IndexWriter.IndexEntriesTable.Entry, out var size);

            var bjro = new BlittableJsonReaderObject(ptr, size, context);
            return bjro;
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }

    public enum BitmapOp
    {
        Or,
        And
    }

    public abstract class QueryOp
    {
        public abstract void Apply(Transaction transaction, Bitmap bitmap, BitmapOp op);

        public abstract override string ToString();

        protected void NoMatches(Bitmap bitmap, BitmapOp op)
        {
            switch (op)
            {
                case BitmapOp.Or:
                    return;
                case BitmapOp.And:
                    bitmap.Clear(); // no results, clear the whole thing
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(op), op, null);
            }
        }

        public abstract bool IsMatch(BlittableJsonReaderObject bjro);
    }
    
    public class BinaryQuery : QueryOp
    {
        private readonly QueryOp[] _queries;
        private readonly BitmapOp _mergeOp;

        public BinaryQuery(QueryOp[] queries, BitmapOp mergeOp)
        {
            _queries = queries;
            _mergeOp = mergeOp;
        }
        
        public override void Apply(Transaction transaction, Bitmap bitmap, BitmapOp op)
        {
            var innerBitmap = new Bitmap();
            _queries[0].Apply(transaction, innerBitmap, BitmapOp.Or);
            for (var index = 1; index < _queries.Length; index++)
            {
                _queries[index].Apply(transaction, innerBitmap, _mergeOp);
            }

            innerBitmap.Apply(bitmap, op);
        }

        public override string ToString()
        {
            return string.Join(_mergeOp.ToString(), _queries.Select(x => x.ToString()));
        }

        public override bool IsMatch(BlittableJsonReaderObject bjro)
        {
            var first = _queries[0].IsMatch(bjro);
            for (int i = 1; i < _queries.Length; i++)
            {
                var cur = _queries[i].IsMatch(bjro);
                if (_mergeOp == BitmapOp.Or)
                    first |= cur;
                else
                    first &= cur;
            }

            return first;
        }
    }

    // where User = $userId and startsWith(Name, 'a')
    public class TermQuery : QueryOp
    {
        private readonly string _field;
        private readonly string _term;

        public TermQuery(string field, string term)
        {
            _field = field;
            _term = term;
        }
        
        public override void Apply(Transaction transaction, Bitmap bitmap, BitmapOp op)
        {
            Tree tree = transaction.ReadTree(_field);
            if (tree == null) // not such field
            {
                NoMatches(bitmap, op);
                return;
            }

            var fst = tree.FixedTreeFor(_term);

            using var it = fst.Iterate();
            if (it.Seek(0) == false) // no matching entries
            {
                NoMatches(bitmap, op);
                return;
            }

            var actual = op == BitmapOp.Or ? bitmap : new Bitmap();
            
            do
            {
                actual.Set(it.CurrentKey);
            } while (it.MoveNext());


            if (op == BitmapOp.And)
            {
                actual.Apply(bitmap, BitmapOp.And);
            }
        }

        public override string ToString()
        {
            return $"{_field} == '{_term}'";
        }

        public override bool IsMatch(BlittableJsonReaderObject bjro)
        {
            if (bjro.TryGetMember(_field, out object val) == false)
                return false;

            if (val is string s)
            {
                return s == _term;
            }
            if (val is LazyStringValue lsv)
            {
                return lsv.ToString() == _term;
            }
            else if (val is BlittableJsonReaderArray a)
            {
                for (int i = 0; i < a.Length; i++)
                {
                    if (a.GetByIndex<string>(i) == _term)
                        return true;
                }
            }

            return false;
        }
    }

    public class Bitmap : IEnumerable<long>
    {
        private readonly SortedList<long, long> _inner = new SortedList<long, long>();

        public void Set(long entryId)
        {
            if (_inner.ContainsKey(entryId) == false)
                _inner[entryId] = entryId;
        }

        public void Clear(long entryId)
        {
            _inner.Remove(entryId);
        }
        
        public void Clear()
        {
            _inner.Clear();
        }

        public void Apply(Bitmap other, BitmapOp op)
        {
            switch (op)
            {
                case BitmapOp.Or:
                    for (int i = 0; i < _inner.Count; i++)
                    {
                        other.Set(_inner.Keys[i]);
                    }
                    break;
                case BitmapOp.And:
                    for (int i = other._inner.Count - 1; i >= 0; i--)
                    {
                        if (_inner.ContainsKey(other._inner.Keys[i]) == false)
                            other._inner.RemoveAt(i);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(op), op, null);
            }
        }

        public IEnumerator<long> GetEnumerator()
        {
            return _inner.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public long Count => _inner.Count;

        public void Remove(long entryId)
        {
            _inner.Remove(entryId);
        }
        
        public long GetAt(long index)
        {
            return _inner.Keys[(int)index];
        }
    }
}
