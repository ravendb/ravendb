﻿using System.Collections.Generic;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct EndsWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private readonly Slice _endsWith;

        private CompactTree.Iterator _iterator;
        public EndsWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, string field, int fieldId, string endsWith)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();

            Slice.From(context, _searcher.EncodeTerm(endsWith, fieldId), out _endsWith);
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
                if (!termSlice.EndsWith(suffix))
                    continue;

                term = _searcher.TermQuery(_field, termSlice.ToString());
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(EndsWithTermProvider)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _field },
                    { "Suffix", _endsWith.ToString()}
                });
        }
    }
}
