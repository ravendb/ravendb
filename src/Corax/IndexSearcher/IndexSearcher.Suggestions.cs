using System.IO;
using Corax.Queries;

namespace Corax;

public partial class IndexSearcher
{
    public IRawTermProvider Suggest(int fieldId, string term, bool sortByPopularity, StringDistanceAlgorithm algorithm, float accuracy,
        int take = Constants.IndexSearcher.TakeAll) => algorithm switch
    {
        StringDistanceAlgorithm.None => Suggest<NoneStringDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.NGram => Suggest<NoneStringDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.JaroWinkler => Suggest<JaroWinklerDistance>(fieldId, term, sortByPopularity, accuracy, take),
        StringDistanceAlgorithm.Levenshtein => Suggest<LevenshteinDistance>(fieldId, term, sortByPopularity, accuracy, take),
        _ => Suggest<LevenshteinDistance>(fieldId, term, sortByPopularity, accuracy, take)
    };

    private SuggestionTermProvider<TDistanceProvider> Suggest<TDistanceProvider>(int fieldId, string term, bool sortByPopularity, float accuracy, int take)
        where TDistanceProvider : IStringDistance
    {
        var termSlice = EncodeAndApplyAnalyzer(term, fieldId);
        if (_fieldMapping.TryGetByFieldId(fieldId, out var binding) == false)
        {
            throw new InvalidDataException($"Field {fieldId} is not indexed.");
        }
        
        return SuggestionTermProvider<TDistanceProvider>.YieldFromNGram(this, fieldId, termSlice, binding, default, sortByPopularity, accuracy, take);
    }

}
