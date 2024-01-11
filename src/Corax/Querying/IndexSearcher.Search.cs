using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Voron;
using Voron.Data.PostingLists;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString()) 
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);

        Analyzer wildcardAnalyzer = null;
        IQueryMatch searchQuery = null;

        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);
        foreach (var word in values)
        {
            var tokensInWord = CountTokens(word, out var token);
            
            //Single word
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
                    termMatches ??= new();

                    terms.Clear(); // Clear the terms list.
                    EncodeAndApplyAnalyzerForMultipleTerms(field, termReadyToAnalyze, ref terms);
                    foreach (var term in terms.GetEnumerator())
                    {
                        if (term.Size == 0)
                            continue; //skip empty results

                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = default;
                
                if (termType is not Constants.Search.SearchMatchOptions.Exists)
                {
                    analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                    if (analyzedTerm.Size == 0)
                        continue; //skip empty results
                }
                
                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException(
                        $"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
                    Constants.Search.SearchMatchOptions.Exists => ExistsQuery(field, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, analyzedTerm, token: cancellationToken),
                    _ => throw new ArgumentOutOfRangeException()
                };

                if (searchQuery is null)
                {
                    searchQuery = query;
                    continue;
                }

                searchQuery = @operator switch
                {
                    Constants.Search.Operator.Or => Or<IQueryMatch, MultiTermMatch>(searchQuery, query, token: cancellationToken),
                    Constants.Search.Operator.And => And<IQueryMatch, MultiTermMatch>(searchQuery, query, token: cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
                };
                
                continue;
            }
            
            //Phrase query
            var startPosition = terms.Count;
            EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);
            if (terms.Count == startPosition) continue; //sentence contained only stop-words

            var hs = new HashSet<Slice>(SliceComparer.Instance);
            for (var i = startPosition; i < terms.Count; ++i)
            {
                hs.Add(terms[i]);
            }

            var allIn = AllInQuery(field, hs, cancellationToken: cancellationToken);

            var phraseMatch = PhraseMatch(allIn, field, terms.ToSpan().Slice(startPosition, terms.Count - startPosition));

            searchQuery = (searchQuery, @operator) switch
            {
                (null, _) => phraseMatch,
                (_, Constants.Search.Operator.And) => And(searchQuery, phraseMatch, cancellationToken),
                (_, Constants.Search.Operator.Or) => Or(searchQuery, phraseMatch, cancellationToken),
            };
            
            terms.Shrink(startPosition);
        }

        if (termMatches?.Count > 0)
        {
            var termMatchesQuery = @operator switch
            {
                Constants.Search.Operator.And => AllInQuery(field, termMatches.ToHashSet(SliceComparer.Instance), skipEmptyItems: true, token: cancellationToken),
                Constants.Search.Operator.Or => InQuery(field, termMatches, token: cancellationToken),
                _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
            };

            if (searchQuery is null)
                searchQuery = termMatchesQuery;
            else
            {
                searchQuery = @operator switch
                {
                    Constants.Search.Operator.Or => Or(termMatchesQuery, searchQuery),
                    Constants.Search.Operator.And => And(termMatchesQuery, searchQuery),
                    _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
                };
            }
        }

        
        void AssertFieldIsSearched()
        {
            if (field.Analyzer == null && field.IsDynamic == false)
                throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
        }
        
        wildcardAnalyzer?.Dispose();

        return searchQuery ?? TermMatch.CreateEmpty(this, Allocator);
        
        
        Constants.Search.SearchMatchOptions GetTermType(ReadOnlySpan<char> termValue)
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

        //In pharse query we expect to have multiple tokens, for most cases 
        int CountTokens(in string source, out Token termToken)
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
        
        IEnumerable<Token> GetTokens(string source)
        {
            if (string.IsNullOrEmpty(source))
            {
                yield return new Token() {Offset = 0, Length = 0};
                yield break;
            }
            
            //TODO This code in from `WhitespaceTokenizer`. We can optimize it later but for now it should be OK.
            int i = 0;

            while (i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                {
                    yield return new Token() {Offset = start, Length = (uint)(i - start), Type = TokenType.Word};
                }
            } 
        }
        
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
