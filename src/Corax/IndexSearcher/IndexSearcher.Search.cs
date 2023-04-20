using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Corax.Mappings;
using Corax.Queries;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString()) 
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);
        
        IQueryMatch searchQuery = null;

        List<Slice> termMatches = null;
        foreach (var value in values)
        {
            var termType = GetTermType(value);
            (int startIncrement, int lengthIncrement) = termType switch
            {
                Constants.Search.SearchMatchOptions.StartsWith => (0, -1),
                Constants.Search.SearchMatchOptions.EndsWith => (1, 0),
                Constants.Search.SearchMatchOptions.Contains => (1, -1),
                Constants.Search.SearchMatchOptions.TermMatch => (0, 0),
                _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
            };

            var termReadyToAnalyze = value.AsSpan(startIncrement, value.Length - startIncrement + lengthIncrement);
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
                Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, analyzedTerm),
                Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, analyzedTerm),
                Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, analyzedTerm),
                _ => throw new ArgumentOutOfRangeException()
            };

            if (searchQuery is null)
            {
                searchQuery = query;
                continue;
            }
            
            searchQuery = @operator switch
            {
                Constants.Search.Operator.Or => Or(searchQuery, query),
                Constants.Search.Operator.And => And(searchQuery, query),
                _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
            };
        }
        
        if (termMatches?.Count > 0)
        {
            var termMatchesQuery = @operator switch
            {
                Constants.Search.Operator.And => AllInQuery(field, termMatches.ToHashSet(SliceComparer.Instance), skipEmptyItems: true),
                Constants.Search.Operator.Or => InQuery(field, termMatches),
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
        
        
        Constants.Search.SearchMatchOptions GetTermType(string termValue)
        {
            if (string.IsNullOrEmpty(termValue))
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
    }
}
