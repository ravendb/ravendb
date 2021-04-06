using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;

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

        public IEnumerable Query(QueryOp q)
        {
            var results = new Bitmap();
            q.Apply(_transaction, results, BitmapOp.Or);

            Table entries = _transaction.OpenTable(IndexWriter.IndexEntriesSchema, IndexWriter.IndexEntriesSlice);
            
            foreach (var entryId in results)
            {
                yield return ExtractDocumentId(entryId);
            }

            unsafe string ExtractDocumentId(long entryId)
            {
                entries.DirectRead(entryId, out TableValueReader tvr);

                byte* ptr = tvr.Read((int)IndexWriter.IndexEntriesTable.DocumentId, out var size);

                var str = Encoding.UTF8.GetString(ptr, size);
                return str;
            }
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
    }

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
