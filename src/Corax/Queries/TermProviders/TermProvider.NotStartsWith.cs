using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct NotStartsWithTermProvider : ITermProvider
    {
        private NotStartsWithTermProvider<CompactTree.ForwardIterator> _inner;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NotStartsWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _inner = new NotStartsWithTermProvider<CompactTree.ForwardIterator>(searcher, context, tree, field, startWith);
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

    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotStartsWithTermProvider<TIterator> : ITermProvider
        where TIterator : struct, ICompactTreeIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;

        private TIterator _iterator;

        public bool IsOrdered => true;

        public NotStartsWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TIterator>();
            _iterator.Reset();
            _startWith = startWith;
            _tree = tree;
        }

        public void Reset()
        {
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var startWith = _startWith.Decoded();
            while (_iterator.MoveNext(out var termScope, out var _))
            {
                var termSlice = termScope.Key.Decoded();
                if (termSlice.StartsWith(startWith))
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
            return new QueryInspectionNode($"{nameof(NotStartsWithTermProvider<TIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
