using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotEndsWithTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly Querying.IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _endsWith;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        private bool _nullsReturned;
        private bool _nonExistingReturned;

        public NotEndsWithTermProvider(Querying.IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey endsWith)
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
            _nullsReturned = false;
            _nonExistingReturned = false;
        }

        public bool Next(out TermMatch term)
        {
            if (_nullsReturned == false)
            {
                _nullsReturned = true;
                if (_searcher.TryGetPostingListForNull(_field, out var nullPostingListId))
                {
                    term = _searcher.TermQuery(_field, nullPostingListId, 1D);
                    return true;
                }
            }

            if (_nonExistingReturned == false)
            {
                _nonExistingReturned = true;
               
                if (_searcher.TryGetPostingListForNonExisting(_field, out var nonExistingPostingListId))
                {
                    term = _searcher.TermQuery(_field, nonExistingPostingListId, 1D);
                    return true;
                }
            }

            var suffix = _endsWith.Decoded();
            while (_iterator.MoveNext(out var key, out _, out _))
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
                    { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                    { Constants.QueryInspectionNode.Suffix, _endsWith.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
