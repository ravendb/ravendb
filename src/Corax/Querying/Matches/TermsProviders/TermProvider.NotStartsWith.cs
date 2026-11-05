using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermsProviders
{
    public struct NotStartsWithTermsProvider<TLookupIterator> : ITermsProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;
        private readonly bool _validatePostfixLen;
        private readonly CancellationToken _token;

        private CompactTree.Iterator<TLookupIterator> _iterator;


        public NotStartsWithTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey startWith, bool validatePostfixLen, CancellationToken token)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _startWith = startWith;
            _validatePostfixLen = validatePostfixLen;
            _token = token;
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            var startWith = _startWith.Decoded();
            int count = 0;

            using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
            var key = scope.Key;

            while (count < postingListIds.Length)
            {
                if (_iterator.MoveNext(key, out long postingListId) == false)
                    break;

                _token.ThrowIfCancellationRequested();
                var termSlice = key.Decoded();

                if (termSlice.StartsWith(startWith))
                {
                    if (_validatePostfixLen == false ||
                        termSlice[^1] == startWith.Length)
                    {
                        continue;
                    }
                }

                postingListIds[count++] = postingListId;
            }

            return count;
        }

        public void Reset()
        {
            _iterator.Reset();
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotStartsWithTermsProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                                { Constants.QueryInspectionNode.Prefix, _startWith.ToString()}
                            });
        }
    }
}
