using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying;
using Corax.Mappings;
using Corax.Pipeline;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron.Impl;
using Encoding = System.Text.Encoding;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxSuggestionReader : SuggestionIndexReaderBase
{
    private readonly IndexFieldsMapping _fieldMappings;
    private readonly IndexSearcher _indexSearcher;
    private readonly IndexFieldBinding _binding;

    public CoraxSuggestionReader(Index index, RavenLogger logger, IndexFieldBinding binding, Transaction readTransaction, IndexFieldsMapping fieldsMapping) : base(index, logger)
    {
        _fieldMappings = fieldsMapping;
        _binding = binding;
        _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings)
        {
            MaxMemoizationSizeInBytes = index.Configuration.MaxMemoizationSize.GetValue(SizeUnit.Bytes) 
        };
    }

    public override SuggestionResult Suggestions(IndexQueryServerSide query, SuggestionField field, JsonOperationContext documentsContext, CancellationToken token)
    {
        var options = field.GetOptions(documentsContext, query.QueryParameters) ?? new SuggestionOptions();
        var terms = field.GetTerms(documentsContext, query.QueryParameters);

        var result = new SuggestionResult { Name = field.Alias ?? field.Name };

        foreach (var suggestion in terms.Count > 1
                     ? QueryOverMultipleWords(field, terms, options)
                     : QueryOverSingleWord(field, terms[0], options))
        {
            result.Suggestions.Add(suggestion.Term);

            if (options.SortMode == SuggestionSortMode.Popularity)
            {
                AddPopularity(suggestion, ref result);
            }
        }

        return result;
    }

    private SuggestWord[] QueryOverMultipleWords(SuggestionField field, List<string> words, SuggestionOptions options)
    {
        var uniqueSuggestions = new HashSet<string>();
        var result = new List<SuggestWord>();

        var pageSize = options.PageSize;
        foreach (var term in words)
        {
            if (pageSize <= 0)
                break;

            foreach (var suggestion in QueryOverSingleWord(field, term, options))
            {
                if (uniqueSuggestions.Add(suggestion.Term) == false)
                    continue;

                result.Add(suggestion);

                pageSize--;
                if (pageSize <= 0)
                    break;
            }
        }

        return result.ToArray();
    }


    private const int MaxTermSize = 128;

    private SuggestWord[] QueryOverSingleWord(SuggestionField field, string word, SuggestionOptions options)
    {
        var sortByPopularity = options.SortMode == SuggestionSortMode.Popularity;
        var match = options.Distance switch
        {
            StringDistanceTypes.JaroWinkler => _indexSearcher.Suggest(_binding.Metadata, word, sortByPopularity, StringDistanceAlgorithm.JaroWinkler,
                options.Accuracy ?? SuggestionOptions.DefaultAccuracy, options.PageSize),
            StringDistanceTypes.NGram => _indexSearcher.Suggest(_binding.Metadata, word, sortByPopularity, StringDistanceAlgorithm.NGram,
                options.Accuracy ?? SuggestionOptions.DefaultAccuracy, options.PageSize),
            _ => _indexSearcher.Suggest(_binding.Metadata, word, sortByPopularity, StringDistanceAlgorithm.Levenshtein,
                options.Accuracy ?? SuggestionOptions.DefaultAccuracy, options.PageSize)
        };

        int minSize = options.PageSize * (Unsafe.SizeOf<Token>() + sizeof(float) + MaxTermSize);
        var buffer = ArrayPool<byte>.Shared.Rent(minSize);

        var bufferSpan = buffer.AsSpan();

        var terms = bufferSpan.Slice(0, MaxTermSize * options.PageSize);
        int position = terms.Length;
        var score = MemoryMarshal.Cast<byte, float>(bufferSpan.Slice(position, sizeof(float) * options.PageSize));
        position += sizeof(float) * options.PageSize;
        var tokens = MemoryMarshal.Cast<byte, Token>(bufferSpan.Slice(position, options.PageSize * Unsafe.SizeOf<Token>()));

        match.Next(ref terms, ref tokens, ref score);

        var list = new List<SuggestWord>();
        for (int i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];

            var suggestWord = new SuggestWord();

            suggestWord.Term = Encoding.UTF8.GetString(terms.Slice(token.Offset, (int)token.Length));

            if (sortByPopularity)
            {
                suggestWord.Score = score[i];
                suggestWord.Freq = (int)_indexSearcher.NumberOfDocumentsUnderSpecificTerm(_binding.Metadata, suggestWord.Term);
            }

            list.Add(suggestWord);
        }

        var result = list.ToArray();
        ArrayPool<byte>.Shared.Return(buffer);

        return result;
    }

    internal virtual void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {

    }

    public override void Dispose()
    {
        _indexSearcher?.Dispose();
    }
}
