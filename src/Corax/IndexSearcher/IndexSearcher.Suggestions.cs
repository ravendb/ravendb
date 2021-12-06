using System;
using Corax.Queries;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public SuggestionTermProvider Suggest(int fieldId, string term, Analyzer analyzer = null, int take = -1)
    {
        // TODO: Until we figure out how to best call this through integration, I will add this convenience method. 
        Slice.From(_transaction.Allocator, term, out var termSlice);
        return Suggest(fieldId, termSlice, analyzer);
    }

    public SuggestionTermProvider Suggest(int fieldId, Slice term, Analyzer analyzer = null, int take = -1)
    {
        return SuggestionTermProvider.YieldFromNGram(this, fieldId, term, analyzer, take);
    }
}
