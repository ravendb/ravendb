using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public enum SearchQueryOptions
    {
        Legacy,
        PhraseQuery,
        PhraseQueryWithWildcardAdjustments
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch SearchQuery(in FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, SearchQueryOptions searchQueryOptions = SearchQueryOptions.PhraseQueryWithWildcardAdjustments, in CancellationToken cancellationToken = default)
    {
        return searchQueryOptions switch
        {
            SearchQueryOptions.Legacy => SearchQueryLegacy(field, values, @operator, cancellationToken),
            SearchQueryOptions.PhraseQueryWithWildcardAdjustments =>
                SearchQueryWithPhraseQueryWithWildcardQueriesAdjustments(field, values, @operator, cancellationToken),
            SearchQueryOptions.PhraseQuery => SearchQueryWithPhraseQuery(field, values, @operator, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(searchQueryOptions))
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AccumulateIntoSearchBitmap(
        IQueryMatch match,
        ref BitmapMatch searchBitmap,
        ref Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData,
        Constants.Search.Operator @operator,
        CancellationToken cancellationToken)
    {
        if (searchBitmap.IsAllocated == false)
        {
            searchBitmap = new BitmapMatch(Allocator);
            tempBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);
            Primitives.QueryPrimitives.OrWithMatch(match, ref searchBitmap.BitmapState, token: cancellationToken);
        }
        else if (@operator == Constants.Search.Operator.Or)
        {
            Primitives.QueryPrimitives.OrWithMatch(match, ref searchBitmap.BitmapState, token: cancellationToken);
        }
        else
        {
            Primitives.QueryPrimitives.AndWithMatch(match, ref searchBitmap.BitmapState, ref tempBitmapData, cancellationToken);
        }
    }

    private void MergeTermMatches(List<Slice> termMatches, FieldMetadata field,
        ref BitmapMatch searchBitmap, ref Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData,
        Constants.Search.Operator @operator, CancellationToken cancellationToken)
    {
        if (termMatches is not { Count: > 0 })
            return;

        var termBitmap = new BitmapMatch(Allocator);
        var tempTermBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);

        if (@operator == Constants.Search.Operator.And)
        {
            Primitives.QueryPrimitives.OrWithMatch(TermQuery(field, termMatches[0]), ref termBitmap.BitmapState, token: cancellationToken);
            for (int index = 1; index < termMatches.Count; index++)
            {
                var termQuery = TermQuery(field, termMatches[index]);
                Primitives.QueryPrimitives.AndWithMatch(termQuery, ref termBitmap.BitmapState, ref tempTermBitmapData, cancellationToken);
            }
        }
        else
        {
            foreach (var term in termMatches)
            {
                Primitives.QueryPrimitives.OrWithMatch(TermQuery(field, term), ref termBitmap.BitmapState, token: cancellationToken);
            }
        }

        AccumulateIntoSearchBitmap(termBitmap, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        tempTermBitmapData.Dispose();
        termBitmap.Dispose();
    }

    private void AccumulatePhraseQuery(FieldMetadata field, ContextBoundNativeList<Slice> terms,
        ref BitmapMatch searchBitmap, ref Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData,
        Constants.Search.Operator @operator, CancellationToken cancellationToken)
    {
        if (terms.Count == 0)
            return;

        var phraseBitmap = new BitmapMatch(Allocator);
        var tempPhraseBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);

        Primitives.QueryPrimitives.OrWithMatch(TermQuery(field, terms[0]), ref phraseBitmap.BitmapState, token: cancellationToken);
        for (int index = 1; index < terms.Count; index++)
        {
            var termQuery = TermQuery(field, terms[index]);
            Primitives.QueryPrimitives.AndWithMatch(termQuery, ref phraseBitmap.BitmapState, ref tempPhraseBitmapData, cancellationToken);
        }

        var phraseMatch = PhraseQuery(phraseBitmap, field, terms.ToSpan());
        tempPhraseBitmapData.Dispose();
        // PhraseQuery does NOT copy phraseBitmap — phraseMatch holds the same BitmapMatch (a struct over the same
        // RoaringBitmap storage). So phraseMatch must be fully consumed by AccumulateIntoSearchBitmap before
        // phraseBitmap is disposed; disposing it earlier frees the storage out from under the Or/And accumulation.
        AccumulateIntoSearchBitmap(phraseMatch, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        phraseBitmap.Dispose();
    }

    /// <summary>Create a wildcard/exists query from the resolved term type and analyzed term.</summary>
    private IQueryMatch CreateWildcardOrExistsQuery(FieldMetadata field,
        Constants.Search.SearchMatchOptions termType, Slice analyzedTerm,
        CancellationToken cancellationToken)
    {
        return termType switch
        {
            Constants.Search.SearchMatchOptions.Exists => ExistsQuery(field, token: cancellationToken),
            Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, analyzedTerm, token: cancellationToken),
            Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, analyzedTerm, token: cancellationToken),
            Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, analyzedTerm, token: cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(termType), termType.ToString())
        };
    }

    /// <summary>Dispose temporaries and return the final search result.</summary>
    private IQueryMatch FinalizeSearchResult(ref BitmapMatch searchBitmap,
        ref Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData)
    {
        tempBitmapData.Dispose();
        if (searchBitmap.IsAllocated == false)
        {
            searchBitmap.Dispose();
            return EmptyQueryMatch.Instance;
        }
        return searchBitmap;
    }

    private IQueryMatch SearchQueryLegacy(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken)
    {
        AssertFieldIsSearched(field);
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString())
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);

        Analyzer wildcardAnalyzer = null;
        BitmapMatch searchBitmap = default;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = default;

        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);
        foreach (var word in values)
        {
            foreach (var token in GetTokens(word))
            {
                var value = word.AsSpan(token.Offset, (int)token.Length);
                var termType = GetTermType(value);
                (int startIncrement, int lengthIncrement, Analyzer analyzer) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0, searchAnalyzer),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0, searchAnalyzer),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);

                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= [];
                    terms.Clear();
                    EncodeAndApplyAnalyzerForMultipleTerms(field, termReadyToAnalyze, ref terms);
                    foreach (var term in terms)
                    {
                        if (term.Size == 0)
                            continue;
                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = default;
                if (termType is not Constants.Search.SearchMatchOptions.Exists)
                {
                    analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                    if (analyzedTerm.Size == 0)
                        continue;
                }

                AccumulateIntoSearchBitmap(
                    CreateWildcardOrExistsQuery(field, termType, analyzedTerm, cancellationToken),
                    ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
            }
        }

        MergeTermMatches(termMatches, field, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        wildcardAnalyzer?.Dispose();
        return FinalizeSearchResult(ref searchBitmap, ref tempBitmapData);

        static IEnumerable<Token> GetTokens(string source)
        {
            if (string.IsNullOrEmpty(source))
            {
                yield return new Token() {Offset = 0, Length = 0};
                yield break;
            }

            int i = 0;
            while (i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                    yield return new Token() {Offset = start, Length = (uint)(i - start), Type = TokenType.Word};
            }
        }
    }


    private IQueryMatch SearchQueryWithPhraseQuery(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched(field);
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString())
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);

        Analyzer wildcardAnalyzer = null;
        BitmapMatch searchBitmap = default;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = default;

        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);
        foreach (var word in values)
        {
            var tokensInWord = CountTokens(word, out var token);

            if (tokensInWord == 0)
                continue;

            // Single word
            if (tokensInWord == 1)
            {
                var value = word.AsSpan(token.Offset, (int)token.Length);
                var termType = GetTermType(value);
                (int startIncrement, int lengthIncrement, Analyzer analyzer) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0, searchAnalyzer),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0, searchAnalyzer),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);

                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= [];
                    terms.Clear();
                    EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);

                    // When single term outputs multiple terms, promote to phrase query
                    if (terms.Count > 1)
                        goto PhraseQuery;

                    foreach (var term in terms)
                    {
                        if (term.Size == 0)
                            continue;
                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = default;
                if (termType is not Constants.Search.SearchMatchOptions.Exists)
                {
                    analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                    if (analyzedTerm.Size == 0)
                        continue;
                }

                AccumulateIntoSearchBitmap(
                    CreateWildcardOrExistsQuery(field, termType, analyzedTerm, cancellationToken),
                    ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
                continue;
            }

            // Phrase query (multi-word input)
            terms.Clear();
            EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);

            if (terms.Count == 0)
                continue; // sentence contained only stop-words
            PhraseQuery:
            AccumulatePhraseQuery(field, terms, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        }

        MergeTermMatches(termMatches, field, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        wildcardAnalyzer?.Dispose();
        return FinalizeSearchResult(ref searchBitmap, ref tempBitmapData);

        static int CountTokens(in string source, out Token termToken)
        {
            int count = 0;
            termToken = default;

            if (string.IsNullOrEmpty(source))
                return count;

            var i = 0;
            while (i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                {
                    termToken = new Token() {Length = (uint)(i - start), Offset = start, Type = TokenType.Word};
                    count++;
                }
            }

            return count;
        }
    }

    private IQueryMatch SearchQueryWithPhraseQueryWithWildcardQueriesAdjustments(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched(field);
        BitmapMatch searchBitmap = default;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = default;
        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);

        foreach (var word in values)
        {
            terms.Clear();
            var termType = GetTermType(word);
            EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);
            var tokensInWord = terms.Count;

            if (tokensInWord == 0)
                continue;

            // Single word (or wildcard StartsWith which is always single-term)
            if (tokensInWord is 1 || termType is Constants.Search.SearchMatchOptions.StartsWith)
            {
                var value = terms[0];
                var valueAsSpan = value.AsSpan();

                // Adjustment to Lucene builder: re-detect term type on analyzed output.
                if (termType is not Constants.Search.SearchMatchOptions.StartsWith)
                    termType = GetTermType(valueAsSpan);

                (int startIncrement, int lengthIncrement) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith when valueAsSpan[^1] != '*' => (0, 0),
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                // Rewrite term without asterisks.
                if (termType is not (Constants.Search.SearchMatchOptions.Exists or Constants.Search.SearchMatchOptions.TermMatch))
                    Slice.From(Allocator, valueAsSpan.Slice(startIncrement, valueAsSpan.Length - startIncrement + lengthIncrement), ByteStringType.Immutable, out value);

                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= [];
                    termMatches.Add(value);
                    continue;
                }

                AccumulateIntoSearchBitmap(
                    CreateWildcardOrExistsQuery(field, termType, value, cancellationToken),
                    ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
                continue;
            }

            // Phrase query (wildcards are not supported in phrase queries)
            AccumulatePhraseQuery(field, terms, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        }

        MergeTermMatches(termMatches, field, ref searchBitmap, ref tempBitmapData, @operator, cancellationToken);
        return FinalizeSearchResult(ref searchBitmap, ref tempBitmapData);
    }

    private static void AssertFieldIsSearched(in FieldMetadata field)
    {
        if (field.Analyzer == null && field.IsDynamic == false)
            throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
    }

    private Constants.Search.SearchMatchOptions GetTermType(ReadOnlySpan<char> termValue)
    {
        if (termValue.IsEmpty)
            return Constants.Search.SearchMatchOptions.TermMatch;

        Constants.Search.SearchMatchOptions mode = default;

        if (termValue[0] == '*')
            mode |= Constants.Search.SearchMatchOptions.EndsWith;

        if (termValue[^1] == '*')
        {
            if (termValue.Length <= 2 || termValue[^2] != '\\')
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
        }

        if (mode == Constants.Search.SearchMatchOptions.Contains && termValue.Count('*') == termValue.Length)
            return Constants.Search.SearchMatchOptions.Exists;

        return mode;
    }

    private Constants.Search.SearchMatchOptions GetTermType(ReadOnlySpan<byte> termValue)
    {
        if (termValue.IsEmpty)
            return Constants.Search.SearchMatchOptions.TermMatch;

        Constants.Search.SearchMatchOptions mode = default;

        if (termValue[0] == '*')
            mode |= Constants.Search.SearchMatchOptions.EndsWith;

        if (termValue[^1] == '*')
        {
            if (termValue.Length <= 2 || termValue[^2] != '\\')
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
        }

        if (mode == Constants.Search.SearchMatchOptions.Contains && termValue.Count((byte)'*') == termValue.Length)
            return Constants.Search.SearchMatchOptions.Exists;

        return mode;
    }

    private Analyzer CreateWildcardAnalyzer(in FieldMetadata field, ref Analyzer analyzer)
    {
        if (analyzer != null)
            return analyzer;
        var a = field.Analyzer.IsExactAnalyzer ? Analyzer.CreateDefaultAnalyzer(Allocator) : Analyzer.CreateLowercaseAnalyzer(Allocator);
        analyzer = a;
        return a;
    }
}
