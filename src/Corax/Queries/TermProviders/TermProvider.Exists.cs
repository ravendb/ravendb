using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Sparrow;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct ExistsTermProvider : ITermProvider
    {
        private ExistsTermProvider<CompactTree.ForwardIterator> _inner;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ExistsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field)
        {
            _inner = new ExistsTermProvider<CompactTree.ForwardIterator>(searcher, tree, field);
        }

        public bool IsOrdered => _inner.IsOrdered;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _inner.Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term)
        {
            return _inner.Next(out term);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            return _inner.GetNextTerm(out term);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
    }

    public unsafe struct ExistsTermProvider<TIterator> : ITermProvider
        where TIterator : struct, ICompactTreeIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;

        private TIterator _iterator;

        public bool IsOrdered => true;

        public ExistsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field)
        {
            _tree = tree;
            _field = field;
            _searcher = searcher;
            _iterator = tree.Iterate<TIterator>();
            _iterator.Reset();
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate<TIterator>();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            while (_iterator.MoveNext(out var keyScope, out var _))
            {
                term = _searcher.TermQuery(_field, keyScope.Key, _tree);
                keyScope.Dispose();
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            while (_iterator.MoveNext(out var keyScope, out var _))
            {
                var key = keyScope.Key.Decoded();
                keyScope.Dispose();
                int termSize = key.Length;
                if (key.Length > 1)
                {
                    if (key[^1] == 0)
                        termSize--;
                }

                term = key.Slice(0, termSize);
                return true;
            }

            term = Span<byte>.Empty;
            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermProvider<TIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() }
                            });
        }
    }
}
