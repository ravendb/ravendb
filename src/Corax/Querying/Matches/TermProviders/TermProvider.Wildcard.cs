using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Corax.Mappings;
using Corax.Pipeline.Parsing;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermProviders;

public struct PatternTermProvider<TLookupIterator> : ITermProvider
    where TLookupIterator : struct, ILookupIterator
{
    private readonly CompactTree _tree;
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly CompactKey _pattern;
    private readonly int _seekPrefixLength;
    private readonly CancellationToken _token;
    private CompactTree.Iterator<TLookupIterator> _iterator;
    
    private readonly string _patternString;
    private readonly ByteString _termBuffer;

    public PatternTermProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey pattern, CancellationToken token)
    {
        _searcher = searcher;
        _field = field;
        _pattern = pattern;
        _token = token;
        _tree = tree;

        var patternSpan = pattern.Decoded();

        // The literal run before the first wildcard character lets us seek instead of scanning the whole field.
        var firstPatternCharacter = patternSpan.IndexOfAny(Constants.Search.PatternSymbols);
        _seekPrefixLength = firstPatternCharacter == -1 ? patternSpan.Length : firstPatternCharacter;

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

    public bool IsFillSupported => false;

    public int Fill(Span<long> containers) => throw new NotImplementedException();

    public void Reset()
    {
        _iterator = _tree.Iterate<TLookupIterator>();

        // Only the forward iterator can seek to the prefix; the backward iterator falls back to a full scan.
        if (_seekPrefixLength > 0 && default(TLookupIterator).IsForward)
            _iterator.Seek(_pattern.Decoded()[.._seekPrefixLength]);
        else
            _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        var pattern = _pattern.Decoded();
        var prefix = default(TLookupIterator).IsForward ? pattern[.._seekPrefixLength] : default;

        while (_iterator.MoveNext(out var compactKey, out _, out _))
        {
            _token.ThrowIfCancellationRequested();

            var key = compactKey.Decoded();

            // Terms are sorted, so once we move past the prefix range there is nothing left to match.
            if (prefix.IsEmpty == false && key.StartsWith(prefix) == false)
                break;

            var isMatch = _patternString != null && StandardParsers.IsAscii(key) == false
                ? IsMatchUtf8(key)
                : IsMatch(pattern, key, Constants.Search.Asterisk, Constants.Search.QuestionMark);

            if (isMatch == false)
                continue;

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }
    
    private bool IsMatchUtf8(ReadOnlySpan<byte> term)
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

        // No '*' at all: the pattern must cover the term exactly.
        if (wildcardPos == -1)
            return pattern.Length == term.Length && Consume(pattern, term, question);

        // Consume prefix (if exists)
        var prefix = pattern[..wildcardPos];
        if (prefix.Length > term.Length || Consume(prefix, term, question) == false)
            return false;

        pattern = pattern[(wildcardPos + 1)..];
        term = term[prefix.Length..];

        while (pattern.IsEmpty == false)
        {
            //search for next wildcard
            wildcardPos = pattern.IndexOf(asterisk);

            // State: [*]patte?rn. So basically, we've to check if there is suffix that accepts our left pattern.
            if (wildcardPos == -1)
            {
                if (pattern.Length > term.Length)
                    return false;

                var suffix = term[^pattern.Length..]; //takes pattern.Length elements from the end
                return Consume(pattern, suffix, question);
            }

            // State [*]pa?t*ern, so we've to find first occurrence of "pa?t"
            var currentSubpattern = pattern[..wildcardPos];
            var subpatternPos = Seek(currentSubpattern, term, question);
            if (subpatternPos == -1)
                return false;

            pattern = pattern[(wildcardPos + 1)..];
            term = term[(subpatternPos + currentSubpattern.Length)..];
        }

        // Pattern ended with '*', which absorbs whatever is left of the term.
        return true;
    }

    // Consumes a fixed-shape piece at the start of the window ('?' matches any single symbol).
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
        // Shortcut, if we do not have question marks, search for the pattern directly.
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
        return new QueryInspectionNode($"{nameof(PatternTermProvider<TLookupIterator>)}",
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                { Constants.QueryInspectionNode.Term, _pattern.ToString()}
            });
    }
}
