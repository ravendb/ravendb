using System;
using Corax.Queries;

namespace Corax;

public partial class IndexSearcher
{
    public BoostingMatch Boost<TInner>(TInner match, float constant)
        where TInner : IQueryMatch
    {
        return BoostingMatch.WithConstant(this, match, constant);
    }
    
    
    public BoostingMatch Boost<TInner>(TInner match, IQueryScoreFunction scoreFunction)
        where TInner : IQueryMatch
    {
        if (scoreFunction.GetType() == typeof(TermFrequencyScoreFunction))
        {
            return BoostingMatch.WithTermFrequency(this, match, (TermFrequencyScoreFunction)(object)scoreFunction);
        }
        else if (scoreFunction.GetType() == typeof(ConstantScoreFunction))
        {
            return BoostingMatch.WithConstant(this, match, (ConstantScoreFunction)(object)scoreFunction);
        }
        else
        {
            throw new NotSupportedException($"Boosting with the score function {scoreFunction.GetType().Name} is not supported.");
        }
    }
}
