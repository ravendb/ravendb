using System.IO;
using Corax.Queries;
using Sparrow.Server.Strings;
using static Corax.Constants;

namespace Corax;

public enum StringDistanceAlgorithm
{
    None,
    NGram,
    JaroWinkler,
    Levenshtein
}
public partial class IndexSearcher
{


    public IRawTermProvider Suggest(int fieldId, string term, bool sortByPopularity, StringDistanceAlgorithm algorithm, 
        float accuracy = Suggestions.DefaultAccuracy,
        int take = Constants.IndexSearcher.TakeAll) => algorithm switch
    {
        StringDistanceAlgorithm.None => Suggest<NoStringDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.NGram => Suggest<NGramDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.JaroWinkler => Suggest<JaroWinklerDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.Levenshtein => Suggest<LevenshteinDistance>(fieldId, term, sortByPopularity, accuracy, take),
        _ => Suggest<LevenshteinDistance>(fieldId, term, sortByPopularity, accuracy, take)
    };

    private SuggestionTermProvider<TDistanceProvider> Suggest<TDistanceProvider>(int fieldId, string term, bool sortByPopularity, float accuracy = Suggestions.DefaultAccuracy, int take = Constants.IndexSearcher.TakeAll)
        where TDistanceProvider : IStringDistance
    {
        var termSlice = EncodeAndApplyAnalyzer(term, fieldId);
        if (_fieldMapping.TryGetByFieldId(fieldId, out var binding) == false)
        {
            throw new InvalidDataException($"Field '{binding.FieldName}' is not indexed for suggestions.");
        }

        return SuggestionTermProvider<TDistanceProvider>.YieldSuggestions(this, fieldId, termSlice, binding, default, sortByPopularity, accuracy, take);        
    }
}
