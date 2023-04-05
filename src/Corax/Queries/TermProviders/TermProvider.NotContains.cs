using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct NotContainsTermProvider : ITermProvider
    {
        private readonly NotContainsTermProvider<CompactTree.ForwardIterator> _inner;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NotContainsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey term)
        {
            _inner = new NotContainsTermProvider<CompactTree.ForwardIterator>(searcher, tree, field, term);
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
        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
    }

    public unsafe struct NotContainsTermProvider<TIterator> : ITermProvider
        where TIterator : struct, ICompactTreeIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _term;

        private TIterator _iterator;

        public bool IsOrdered => true;

        public NotContainsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey term)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TIterator>();
            _iterator.Reset();
            _term = term;
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate<TIterator>();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var contains = _term.Decoded();
            while (_iterator.MoveNext(out var termScope, out var _))
            {
                var termSlice = termScope.Key.Decoded();
                if (termSlice.Contains(contains))
                {
                    termScope.Dispose();
                    continue;
                }

                term = _searcher.TermQuery(_field, termScope.Key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotContainsTermProvider<TIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Term", _term.ToString()}
                            });
        }
    }
}
