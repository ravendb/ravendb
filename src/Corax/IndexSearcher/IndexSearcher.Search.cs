using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Corax.Mappings;
using Corax.Queries;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(FieldMetadata field, List<string> termMatches, List<(string Term, Constants.Search.SearchMatchOptions Match)> prefixSuffixMatches, Constants.Search.Operator @operator)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString()) 
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);
        
        IQueryMatch searchQuery = null;
        
        if (termMatches?.Count > 0)
        {
            searchQuery = @operator switch
            {
                Constants.Search.Operator.And => AllInQuery(field, termMatches.ToHashSet(), skipEmptyItems: true),
                Constants.Search.Operator.Or => InQuery(field, termMatches),
                _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
            };
        }

        for (int termIdx = 0; termIdx < prefixSuffixMatches?.Count; ++termIdx)
        {
            var currentItem = prefixSuffixMatches[termIdx];

            (int startIncrement, int lengthIncrement) = currentItem.Match switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };
            
            var termReadyToAnalyze = prefixSuffixMatches[termIdx].Term.AsSpan(startIncrement, currentItem.Term.Length - startIncrement + lengthIncrement);
            var term = EncodeAndApplyAnalyzer(field, termReadyToAnalyze);

            var query = prefixSuffixMatches[termIdx].Match switch
            {
                Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException($"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
                Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, term),
                Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, term),
                Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, term),
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

        void AssertFieldIsSearched()
        {
            if (field.Analyzer == null && field.IsDynamic == false)
                throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
        }

        return searchQuery ?? TermMatch.CreateEmpty(this, Allocator);
    }
}
