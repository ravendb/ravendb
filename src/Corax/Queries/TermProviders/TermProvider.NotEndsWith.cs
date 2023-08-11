using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using CompactTreeForwardIterator = Voron.Data.CompactTrees.CompactTree.Iterator<Voron.Data.Lookups.Lookup<Voron.Data.CompactTrees.CompactTree.CompactKeyLookup>.ForwardIterator>;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotEndsWithTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _endsWith;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        public NotEndsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey endsWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _endsWith = endsWith;
            _tree = tree;
        }

        public bool IsFillSupported { get; }

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var suffix = _endsWith.Decoded();
            while (_iterator.MoveNext(out var key, out var _))
            {
                var termSlice = key.Decoded();
                if (termSlice.EndsWith(suffix))
                {
                    continue;
                }

                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotEndsWithTermProvider<TLookupIterator>)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _field.ToString() },
                    { "Terms", _endsWith.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
