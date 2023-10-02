using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying;

partial class IndexSearcher
{
    public MemoizationMatchProvider<TInner> Memoize<TInner>(TInner inner)
        where TInner : IQueryMatch
    {
        return new MemoizationMatchProvider<TInner>(this, inner);
    }
}
