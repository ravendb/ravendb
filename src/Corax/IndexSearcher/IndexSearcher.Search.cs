using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Queries;
using Sparrow;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString()) 
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);
        
        IQueryMatch searchQuery = null;

        List<Slice> termMatches = null;
        foreach (var word in values)
        {
            foreach (var token in GetTokens(word))
            {
                var value = word.AsSpan(token.Offset, (int)token.Length);
                var termType = GetTermType(value);
                (int startIncrement, int lengthIncrement) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);
                var analyzedTerm = EncodeAndApplyAnalyzer(field, termReadyToAnalyze, canReturnEmptySlice: true);

                if (analyzedTerm.Size == 0)
                    continue; //skip empty results
            
                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= new();
                    termMatches.Add(analyzedTerm);
                    continue;
                }
            
                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException($"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
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
                    Constants.Search.Operator.Or => Or(searchQuery, query, token: cancellationToken),
                    Constants.Search.Operator.And => And(searchQuery, query, token: cancellationToken),
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
}
