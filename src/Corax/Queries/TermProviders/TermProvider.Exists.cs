using System;
using System.Collections.Generic;
using Corax.Mappings;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Queries.TermProviders
{
    public struct ExistsTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        public ExistsTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field)
        {
            _tree = tree;
            _field = field;
            _searcher = searcher;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
        }

        public bool IsFillSupported { get; }

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {            
            _iterator = _tree.Iterate<TLookupIterator>();
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            while (_iterator.MoveNext(out var key, out var _))
            {
                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            while (_iterator.MoveNext(out var compactKey, out var _))
            {
                var key = compactKey.Decoded();
                int termSize = key.Length;
                if (key.Length > 1)
                {
                    if (key[^1] == 0)
                        termSize--;
                }

                term = key.Slice(0, termSize);
                return true;
            }

            term = Span<byte>.Empty;
            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() }
                            });
        }
    }
}
