using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct DistinctTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private readonly Slice _term;
        private CompactTree.Iterator _iterator;
        
        public DistinctTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, string field, Slice term)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _term = term;            
        }

        public void Reset()
        {
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var distinct = _term.AsReadOnlySpan();
            while (_iterator.MoveNext(out Slice termSlice, out var _))
            {
                if (MemoryExtensions.SequenceCompareTo(distinct, termSlice) == 0)
                    continue;

                term = _searcher.TermQuery(_field, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(DistinctTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field },
                                { "Term", string.Join(",", _term)}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
