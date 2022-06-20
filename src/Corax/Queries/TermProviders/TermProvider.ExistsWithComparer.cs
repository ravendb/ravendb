using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public unsafe struct ExistsTermProviderWithComparer<TComparer> : ITermProvider
    where TComparer : UnaryMatch<IQueryMatch,Slice>.IUnaryMatchComparer
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private CompactTree.Iterator _iterator;
        private readonly TComparer _comparer;
        private readonly Slice _value;
        
        public ExistsTermProviderWithComparer(IndexSearcher searcher, ByteStringContext context, CompactTree tree, TComparer comparer, string field, Slice value)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _comparer = comparer;
            _value = value;
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
                if (_comparer.Compare(termSlice, _value) == false)
                    continue;
                term = _searcher.TermQuery(_field, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            while (_iterator.MoveNext(out Span<byte> termSlice, out var _))
            {
                // This shouldnt happen.
                if (termSlice.Length < 1)
                    continue;

                int termSize = termSlice.Length;
                if (termSlice[^1] == 0)
                    termSize--;

                term = termSlice.Slice(0, termSize);
                return true;
            }

            term = Span<byte>.Empty;
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
