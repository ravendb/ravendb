using System.Collections.Generic;
using Corax.Mappings;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct EndsWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly Slice _endsWith;
        private readonly FieldMetadata _field;
        private CompactTree.Iterator _iterator;
        public EndsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice endsWith)
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
            var suffix = _endsWith;
            while (_iterator.MoveNext(out Slice termSlice, out var _))
            {
                if (termSlice.EndsWith(suffix) == false)
                    continue;

                term = _searcher.TermQuery(_field, _tree, termSlice);
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
