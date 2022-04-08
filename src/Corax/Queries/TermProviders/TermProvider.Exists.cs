using System;
using System.Collections.Generic;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct ExistsTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private CompactTree.Iterator _iterator;

        public ExistsTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, string field)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            while (_iterator.MoveNext(out Slice termSlice, out var _))
            {
                term = _searcher.TermQuery(_field, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }

        public bool GetNextTerm(out Slice termSlice)
        {
            while (_iterator.MoveNext(out termSlice, out var _))
            {
                return true;
            }

            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field }
                            });
        }
    }
}
