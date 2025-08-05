using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Indexing;

public partial class IndexWriter
{
    /// <summary>
    /// FieldBuffers are used to prepare field terms in sorted order without allocating native memory and do not changing the orders of IndexedField properties since we're linking them via positions in buffers.
    /// 
    /// </summary>
    private class FieldBuffers<TKey, TLookupKey> : IDisposable
    where TKey : unmanaged
    where TLookupKey : struct, ILookupKey
    {
        private readonly IndexWriter _parent;
        public const int BatchSize = 1024;

        private TKey[] _sortedTerms;
        private int[] _termIndexes;

        public TLookupKey[] Keys;
        public int[] PageOffsets;
        public long[] PostListIds;
        private int[] _entriesOffsets;

        public void PrepareTerms(IndexedField field, out Span<TKey> terms, out Span<int> indexes)
        {
            int termsCount;
            if (typeof(TKey) == typeof(Slice))
                termsCount = field.Textual.Count;
            else if (typeof(TKey) == typeof(long))
                termsCount = field.Longs.Count;
            else if (typeof(TKey) == typeof(double))
                termsCount = field.Doubles.Count;
            else
                throw new InvalidDataException($"Type {typeof(TKey).FullName} is not supported");
            
            if (_sortedTerms == null || _sortedTerms.Length < termsCount)
            {
                if (_sortedTerms != null)
                {
                    ArrayPool<TKey>.Shared.Return(_sortedTerms);
                    ArrayPool<int>.Shared.Return(_termIndexes);
                }

                _sortedTerms = ArrayPool<TKey>.Shared.Rent(termsCount);
                _termIndexes = ArrayPool<int>.Shared.Rent(termsCount);
            }

            int idx = 0;
            if (typeof(TKey) == typeof(Slice))
            {
                foreach (var (k, v) in field.Textual)
                {
                    _sortedTerms[idx] = (TKey)(object)k;
                    _termIndexes[idx] = v;
                    idx++;
                }
            }
            if (typeof(TKey) == typeof(long))
            {
                foreach (var (k, v) in field.Longs)
                {
                    _sortedTerms[idx] = (TKey)(object)k;
                    _termIndexes[idx] = v;
                    idx++;
                }
            }
            if (typeof(TKey) == typeof(double))
            {
                foreach (var (k, v) in field.Doubles)
                {
                    _sortedTerms[idx] = (TKey)(object)k;
                    _termIndexes[idx] = v;
                    idx++;
                }
            }

            terms = new Span<TKey>(_sortedTerms, 0, termsCount);
            indexes = new Span<int>(_termIndexes, 0, termsCount);

            if (typeof(TKey) == typeof(Slice))
                (MemoryMarshal.Cast<TKey, Slice>(terms)).Sort(indexes, SliceComparer.Instance);
            else if (typeof(TKey) == typeof(long))
                (MemoryMarshal.Cast<TKey, long>(terms)).Sort(indexes);
            else if (typeof(TKey) == typeof(double))
                (MemoryMarshal.Cast<TKey, double>(terms)).Sort(indexes);
            else
                throw new InvalidDataException($"Type {typeof(TKey).FullName} is not supported");
        }

        public FieldBuffers(IndexWriter parent)
        {
            _parent = parent;
            Keys = ArrayPool<TLookupKey>.Shared.Rent(BatchSize);
            PageOffsets = ArrayPool<int>.Shared.Rent(BatchSize);
            PostListIds = ArrayPool<long>.Shared.Rent(BatchSize);
            _entriesOffsets = ArrayPool<int>.Shared.Rent(BatchSize);
        }

        public void Dispose()
        {
            if (PostListIds != null) ArrayPool<long>.Shared.Return(PostListIds);
            if (PageOffsets != null) ArrayPool<int>.Shared.Return(PageOffsets);
            if (_entriesOffsets != null) ArrayPool<int>.Shared.Return(_entriesOffsets);

            if (_sortedTerms != null) ArrayPool<TKey>.Shared.Return(_sortedTerms);
            if (_termIndexes != null) ArrayPool<int>.Shared.Return(_termIndexes);

            if (Keys != null && typeof(TLookupKey) == typeof(CompactTree.CompactKeyLookup))
            {
                var llt = _parent._transaction.LowLevelTransaction;
                var ctk = (CompactTree.CompactKeyLookup[])(object)Keys;
                for (int i = 0; i < Keys.Length; i++)
                {
                    ref var k = ref ctk[i].Key;
                    if (k != null)
                    {
                        llt.ReleaseCompactKey(ref k);
                    }
                }
            }
            
            if (Keys != null)
                ArrayPool<TLookupKey>.Shared.Return(Keys);

            PostListIds = null;
            PageOffsets = null;
            _entriesOffsets = null;
            Keys = null;
        }
    }
}
