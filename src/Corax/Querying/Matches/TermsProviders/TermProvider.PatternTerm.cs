using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Corax.Mappings;
using Corax.Pipeline.Parsing;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermsProviders
{
    /// <summary>
    /// Glob matching over a field's terms: '*' absorbs any run, '?' matches exactly one symbol. The literal run
    /// before the first wildcard is used as a seek prefix so we scan a block rather than the whole field.
    /// </summary>
    [DebuggerDisplay("{DebugView,nq}")]
    public struct PatternTermsProvider<TLookupIterator> : ITermsProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _pattern;
        private readonly CompactKey _seekLimitForBackward;
        private readonly int _seekPrefixLength;
        private readonly CancellationToken _token;
        private CompactTree.Iterator<TLookupIterator> _iterator;
        private bool _firstRun;

        // Only populated when the pattern carries '?', which has to be compared per character, not per byte.
        private readonly string _patternString;
        private readonly ByteString _termBuffer;

        public PatternTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey pattern, CompactKey seekLimitForBackward, CancellationToken token)
        {
            _searcher = searcher;
            _field = field;
            _pattern = pattern;
            _seekLimitForBackward = seekLimitForBackward;
            _token = token;
            _tree = tree;

            var patternSpan = pattern.Decoded();

            _seekPrefixLength = patternSpan.IndexOfAny(Constants.Search.PatternSymbols);
            if (_seekPrefixLength < 0)
                throw new InvalidOperationException($"{nameof(PatternTermsProvider<TLookupIterator>)} must be used with at least one pattern symbol.");

            _patternString = null;
            _termBuffer = default;
            if (patternSpan.Contains(Constants.Search.QuestionMark))
            {
                _patternString = Encoding.UTF8.GetString(patternSpan);
                searcher.Allocator.Allocate(Constants.Terms.MaxLength * sizeof(char), out _termBuffer);
            }

            _iterator = tree.Iterate<TLookupIterator>();

            Reset();
        }

        /// <summary>
        /// The forward iterator seeks the literal prefix itself; the backward one seeks the term just past the
        /// prefix range and walks down towards it, so it is only bounded when that limit was prepared for us.
        /// </summary>
        private bool IsBoundedByPrefix => default(TLookupIterator).IsForward
            ? _seekPrefixLength > 0
            : _seekLimitForBackward != null;

        public int FillPostingListIds(Span<long> postingListIds)
        {
            var pattern = _pattern.Decoded();
            var prefix = IsBoundedByPrefix ? pattern[.._seekPrefixLength] : default;
            int count = 0;

            using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
            var compactKey = scope.Key;

            while (count < postingListIds.Length)
            {
                if (_iterator.MoveNext(compactKey, out long postingListId) == false)
                    break;

                _token.ThrowIfCancellationRequested();

                var key = compactKey.Decoded();
                var isFirstTerm = _firstRun;
                _firstRun = false;

                // Terms are sorted, so once we leave the prefix block there is nothing left to match.
                if (prefix.IsEmpty == false && key.StartsWith(prefix) == false)
                {
                    // The backward iterator starts at the first term past our range, so that very first term is
                    // allowed to miss: it is either the boundary term itself or, when the boundary is not stored,
                    // a term below the whole range. Any later miss means we have left the range for good.
                    if (default(TLookupIterator).IsForward == false && isFirstTerm)
                        continue;

                    break;
                }

                var isMatch = _patternString != null && StandardParsers.IsAscii(key) == false
                    ? IsMatchUtf8(key)
                    : IsMatch(pattern, key, Constants.Search.Asterisk, Constants.Search.QuestionMark);

                if (isMatch == false)
                    continue;

                postingListIds[count++] = postingListId;
            }

            return count;
        }

        public void Reset()
        {
            _iterator = _tree.Iterate<TLookupIterator>();
            _firstRun = true;

            if (IsBoundedByPrefix == false)
            {
                // Nothing to seek to, so the whole field has to be scanned.
                _iterator.Reset();
                return;
            }

            if (default(TLookupIterator).IsForward)
            {
                _iterator.Seek(_pattern.Decoded()[.._seekPrefixLength]);
                return;
            }

            _iterator.Seek(_seekLimitForBackward);
        }

        private readonly bool IsMatchUtf8(ReadOnlySpan<byte> term)
        {
            var termChars = _termBuffer.ToSpan<char>();
            if (Encoding.UTF8.TryGetChars(term, termChars, out var charsWritten) == false)
                throw new InvalidOperationException(
                    $"A term did not fit into {termChars.Length} chars during wildcard matching. Corax terms are limited to {Constants.Terms.MaxLength} bytes.");

            return IsMatch(_patternString, termChars[..charsWritten],
                (char)Constants.Search.Asterisk, (char)Constants.Search.QuestionMark);
        }

        private static bool IsMatch<T>(ReadOnlySpan<T> pattern, ReadOnlySpan<T> term, T asterisk, T question)
            where T : struct, IEquatable<T>
        {
            var wildcardPos = pattern.IndexOf(asterisk);

            // No asterisk at all: the pattern has to cover the term exactly.
            if (wildcardPos == -1)
                return pattern.Length == term.Length && Consume(pattern, term, question);

            var prefix = pattern[..wildcardPos];
            if (prefix.Length > term.Length || Consume(prefix, term, question) == false)
                return false;

            pattern = pattern[(wildcardPos + 1)..];
            term = term[prefix.Length..];

            while (pattern.IsEmpty == false)
            {
                wildcardPos = pattern.IndexOf(asterisk);

                // No asterisk left, so the rest of the pattern has to match the tail of the term.
                if (wildcardPos == -1)
                {
                    if (pattern.Length > term.Length)
                        return false;

                    var suffix = term[^pattern.Length..];
                    return Consume(pattern, suffix, question);
                }

                // Another asterisk ahead, so find the first occurrence of the piece before it.
                var currentSubpattern = pattern[..wildcardPos];
                var subpatternPos = Seek(currentSubpattern, term, question);
                if (subpatternPos == -1)
                    return false;

                pattern = pattern[(wildcardPos + 1)..];
                term = term[(subpatternPos + currentSubpattern.Length)..];
            }

            // The pattern ended with an asterisk, which absorbs whatever is left of the term.
            return true;
        }

        // Consumes a fixed-shape piece at the start of the window; the question mark matches any single symbol.
        private static bool Consume<T>(ReadOnlySpan<T> pattern, ReadOnlySpan<T> term, T questionMark)
            where T : struct, IEquatable<T>
        {
            for (var i = 0; i < pattern.Length; i++)
            {
                if (pattern[i].Equals(term[i]) == false && pattern[i].Equals(questionMark) == false)
                    return false;
            }

            return true;
        }

        private static int Seek<T>(ReadOnlySpan<T> pattern, ReadOnlySpan<T> term, T questionMark)
            where T : struct, IEquatable<T>
        {
            // No question mark in the sub-pattern, so a direct search is enough.
            if (pattern.Contains(questionMark) == false)
                return term.IndexOf(pattern);

            for (var pos = 0; pos + pattern.Length <= term.Length; pos++)
            {
                if (Consume(pattern, term.Slice(pos, pattern.Length), questionMark))
                    return pos;
            }

            return -1;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(PatternTermsProvider<TLookupIterator>)}",
                parameters: new Dictionary<string, string>()
                {
                    { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                    { Constants.QueryInspectionNode.Term, _pattern.ToString() },
                    { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName<TLookupIterator>() }
                });
        }

        public string DebugView => Inspect().ToString();
    }
}
