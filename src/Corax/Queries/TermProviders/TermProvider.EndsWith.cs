using System;
using System.Collections.Generic;
using Corax.Mappings;
using Voron.Data.CompactTrees;

using CompactTreeForwardIterator = Voron.Data.CompactTrees.CompactTree.Iterator<Voron.Data.Lookups.Lookup<Voron.Data.CompactTrees.CompactTree.CompactKeyLookup>.ForwardIterator>;

namespace Corax.Queries
{
    public struct EndsWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;

        private readonly CompactKey _endsWith;

        private CompactTreeForwardIterator _iterator;
        
        public bool IsOrdered => true;

        public EndsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey endsWith)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _endsWith = endsWith;
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var suffix = _endsWith.Decoded();
            while (_iterator.MoveNext(out var key, out var _))
            {
                var termSlice = key.Decoded();
                if (termSlice.EndsWith(suffix) == false)
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
            return new QueryInspectionNode($"{nameof(EndsWithTermProvider)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _field.ToString() },
                    { "Suffix", _endsWith.ToString()}
                });
        }
    }
}
