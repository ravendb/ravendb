using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries.MultiTermMatch.TermProviders
{
    public unsafe struct ExistsTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly Slice _fieldName;
        private CompactTree.Iterator _iterator;

        public ExistsTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, Slice fieldName)
        {
            _tree = tree;
            _searcher = searcher;
            _fieldName = fieldName;
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
                term = _searcher.TermQuery(_tree, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            while (_iterator.MoveNext(out Span<byte> termSlice, out var _))
            {
                Debug.Assert(termSlice.Length > 1);
                
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
                                { "Field", _fieldName.ToString() }
                            });
        }
    }
}
