using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Parquet.Thrift;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;
using Encoding = System.Text.Encoding;
using Type = System.Type;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxSuggestionReader : SuggestionIndexReaderBase
{
    private readonly IndexFieldsMapping _fieldMappings;
    private readonly IndexSearcher _indexSearcher;
    private readonly IndexFieldBinding _binding;

    public CoraxSuggestionReader(Index index, Logger logger, IndexFieldBinding binding, Transaction readTransaction) : base(index, logger)
    {
        _fieldMappings = CoraxDocumentConverterBase.GetKnownFields(readTransaction.Allocator, index);
        _fieldMappings.UpdateAnalyzersInBindings(CoraxIndexingHelpers.CreateCoraxAnalyzers(readTransaction.Allocator, index, index.Definition, true));
        _binding = binding;
        _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
    }

    public override SuggestionResult Suggestions(IndexQueryServerSide query, SuggestionField field, JsonOperationContext documentsContext, CancellationToken token)
    {
        var options = field.GetOptions(documentsContext, query.QueryParameters) ?? new SuggestionOptions();
        var terms = field.GetTerms(documentsContext, query.QueryParameters);

        var result = new SuggestionResult { Name = field.Alias ?? field.Name };

        result.Suggestions.AddRange(terms.Count > 1
            ? QueryOverMultipleWords(field, terms, options)
            : QueryOverSingleWord(field, terms[0], options));

        return result;
    }

    private string[] QueryOverMultipleWords(SuggestionField field, List<string> words, SuggestionOptions options)
    {
        var result = new HashSet<string>();

        var pageSize = options.PageSize;
        foreach (var term in words)
        {
            if (pageSize <= 0)
                break;

            foreach (var suggestion in QueryOverSingleWord(field, term, options))
            {
                if (result.Add(suggestion) == false)
                    continue;

                pageSize--;
                if (pageSize <= 0)
                    break;
            }
        }

        return result.ToArray();
    }


    private const int MaxTermSize = 128;

    private unsafe string[] QueryOverSingleWord(SuggestionField field, string word, SuggestionOptions options)
    {
        var sortByPopularity = options.SortMode == SuggestionSortMode.Popularity;
        var match = options.Distance switch
        {
            StringDistanceTypes.JaroWinkler => _indexSearcher.Suggest(_binding.FieldId, word, sortByPopularity, StringDistanceAlgorithm.JaroWinkler,
                options.Accuracy ?? SuggestionOptions.DefaultAccuracy, options.PageSize),
            StringDistanceTypes.NGram => _indexSearcher.Suggest(_binding.FieldId, word, sortByPopularity, StringDistanceAlgorithm.NGram,
                options.Accuracy ?? SuggestionOptions.DefaultAccuracy, options.PageSize),
            _ => _indexSearcher.Suggest(_binding.FieldId, word, sortByPopularity, StringDistanceAlgorithm.Levenshtein,
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

        var list = new List<string>();
        foreach (var token in tokens)
        {
            list.Add(Encoding.UTF8.GetString(terms.Slice(token.Offset, (int)token.Length)));
        }

        var result = list.ToArray();
        ArrayPool<byte>.Shared.Return(buffer);

        return result;
    }



    public override void Dispose()
    {
        _indexSearcher?.Dispose();
    }
}
