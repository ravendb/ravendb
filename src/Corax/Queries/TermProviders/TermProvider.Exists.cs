using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Queries.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;

namespace Corax.Queries.TermProviders
{
    public struct ExistsTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher.IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        
        
        private readonly bool _nullExists;
        private readonly PostingList _nullPostingList;
        private PostingList.Iterator _nullIterator;
        private bool _fetchNulls;
        private long _postingListId;
        
        private CompactTree.Iterator<TLookupIterator> _iterator;

        public ExistsTermProvider(IndexSearcher.IndexSearcher searcher, CompactTree tree, FieldMetadata field)
        {
            _tree = tree;
            _field = field;
            _searcher = searcher;
            if ((_nullExists = _searcher.TryGetPostingListForNull(field, out  _postingListId)) == false)
                _nullIterator = default;
            else
            {
                _nullPostingList = searcher.GetPostingList(_postingListId);
                _nullIterator = _nullPostingList.Iterate();
                _fetchNulls = true;
            }
            
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
        }

        public bool IsFillSupported => true;

        public int Fill(Span<long> containers)
        {
            if (_fetchNulls)
            {
                if (_nullIterator.Fill(containers, out var total))
                {
                    return total;
                }
                
                _fetchNulls = false;
            }
            
            return _iterator.Fill(containers);
        }

        public void Reset()
        {
            _fetchNulls = _nullExists;
            if (_fetchNulls)
                _nullIterator = _nullPostingList.Iterate();

            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            if (_fetchNulls)
            {
                _fetchNulls = false;
                term = _searcher.TermQuery(_field, containerId: _postingListId, 1D);
                return true;
            }
            
            while (_iterator.MoveNext(out var key, out _, out _))
            {
                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            if (_fetchNulls)
            {
                term = Constants.ProjectionNullValueSlice;
                _fetchNulls = false;
                return true;
            }
            
            while (_iterator.MoveNext(out var compactKey, out _, out _))
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
