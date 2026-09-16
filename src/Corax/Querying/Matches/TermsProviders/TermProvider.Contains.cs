using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermsProviders
{
    public struct ContainsTermsProvider<TLookupIterator> : ITermsProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _term;

        private CompactTree.Iterator<TLookupIterator> _iterator;


        public ContainsTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey term)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _term = term;
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            var contains = _term.Decoded();
            int count = 0;

            using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
            var key = scope.Key;

            while (count < postingListIds.Length)
            {
                if (_iterator.MoveNext(key, out long postingListId) == false)
                    break;

                if (!key.Decoded().Contains(contains))
                    continue;

                postingListIds[count++] = postingListId;
            }

            return count;
        }

        public void Reset()
        {
            _iterator = _tree.Iterate<TLookupIterator>();
            _iterator.Reset();
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ContainsTermsProvider<>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                                { Constants.QueryInspectionNode.Term, _term.ToString()}
                            });
        }
    }
}
