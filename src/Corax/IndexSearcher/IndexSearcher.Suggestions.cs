using System.IO;
using Corax.Mappings;
using Corax.Queries;
using Sparrow.Server.Strings;
using static Corax.Constants;

namespace Corax.IndexSearcher;

public enum StringDistanceAlgorithm
{
    None,
    NGram,
    JaroWinkler,
    Levenshtein
}
public partial class IndexSearcher
{
    public IRawTermProvider Suggest(FieldMetadata field, string term, bool sortByPopularity, StringDistanceAlgorithm algorithm, 
        float accuracy = Suggestions.DefaultAccuracy,
        int take = Constants.IndexSearcher.TakeAll) => algorithm switch
    {
        StringDistanceAlgorithm.None => Suggest<NoStringDistance>(field, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.NGram => Suggest<NGramDistance>(field, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.JaroWinkler => Suggest<JaroWinklerDistance>(field, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.Levenshtein => Suggest<LevenshteinDistance>(field, term, sortByPopularity, accuracy, take),
        _ => Suggest<LevenshteinDistance>(field, term, sortByPopularity, accuracy, take)
    };

    private SuggestionTermProvider<TDistanceProvider> Suggest<TDistanceProvider>(FieldMetadata field, string term, bool sortByPopularity, float accuracy = Suggestions.DefaultAccuracy, int take = Constants.IndexSearcher.TakeAll)
        where TDistanceProvider : IStringDistance
    {
        var termSlice = EncodeAndApplyAnalyzer(field, term);
        if (_fieldMapping.TryGetByFieldId(field.FieldId, out var binding) == false)
        {
            throw new InvalidDataException($"Field '{binding.FieldName}' is not indexed for suggestions.");
        }

        return SuggestionTermProvider<TDistanceProvider>.YieldSuggestions(this, binding.FieldId, termSlice, binding, default, sortByPopularity, accuracy, take);        
    }
}
