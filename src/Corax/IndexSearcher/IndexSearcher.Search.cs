using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Queries;
using Corax.Queries.Meta;
using Sparrow.Server;
using Voron;
using Voron.Data.PostingLists;
using Voron.Util;

namespace Corax.IndexSearcher;

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
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);

              
                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= new();
                    var terms = new NativeUnmanagedList<Slice>(Allocator, 8);
                    EncodeAndApplyAnalyzerForMultipleTerms(field, termReadyToAnalyze, ref terms);
                    var termsSpan = terms.Items;
                    foreach (var term in termsSpan)
                    {
                        if (term.Size == 0)
                            continue; //skip empty results
                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                if (analyzedTerm.Size == 0)
                    continue; //skip empty results
                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException(
                        $"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
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
            }
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
                    Constants.Search.Operator.Or => Or<IQueryMatch, IQueryMatch>(termMatchesQuery, searchQuery),
                    Constants.Search.Operator.And => And<IQueryMatch, IQueryMatch>(termMatchesQuery, searchQuery),
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
                if (termValue[^2] != '\\')
                    mode |= Constants.Search.SearchMatchOptions.StartsWith;
            }

            return mode;
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
