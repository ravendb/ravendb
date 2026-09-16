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
    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartsWithTermsProvider<TLookupIterator> : ITermsProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;
        private readonly CompactKey _startWithLimit;
        private readonly bool _validatePostfixLen;
        private readonly CancellationToken _token;
        private bool _firstRun;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        public StartsWithTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey startWith, CompactKey seekTerm, bool validatePostfixLen, CancellationToken token)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _startWith = startWith;
            _startWithLimit = seekTerm;
            _validatePostfixLen = validatePostfixLen;
            _token = token;

            Reset();
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            ReadOnlySpan<byte> decodedStartsWith = _startWith.Decoded();
            int count = 0;

            using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
            var compactKey = scope.Key;

            while (count < postingListIds.Length)
            {
                if (_iterator.MoveNext(compactKey, out long postingListId ) == false)
                    break;

                var key = compactKey.Decoded();
                if (_validatePostfixLen && key[^1] != decodedStartsWith.Length)
                {
                    _token.ThrowIfCancellationRequested();
                    continue;
                }

                if (_firstRun && default(TLookupIterator).IsForward == false && key.StartsWith(decodedStartsWith) == false)
                {
                    _firstRun = false;
                    continue;
                }

                if (key.StartsWith(decodedStartsWith) == false)
                    break;

                postingListIds[count++] = postingListId;
            }

            return count;
        }

        public void Reset()
        {
            if (default(TLookupIterator).IsForward)
            {
                _iterator.Seek(_startWith);
                return;
            }

            _firstRun = true;

            // Backward scan: seek to the END of the prefix block (successor of the prefix) and walk down. When the
            // prefix has no finite successor (empty / all-0xFF), the block runs to the very end of the tree, so there
            // is no key to seek — Reset positions the backward iterator at the last entry instead. The first MoveNext
            // then yields the largest in-prefix key (or the _firstRun skip discards the overshoot when the successor
            // happened to be a stored key).
            if (_startWithLimit is null)
                _iterator.Reset();
            else
                _iterator.Seek(_startWithLimit);
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartsWithTermsProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                                { Constants.QueryInspectionNode.Prefix, _startWith.ToString()},
                                { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName<TLookupIterator>()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
