using Corax.Queries;
using Corax.Queries.Meta;

namespace Corax.IndexSearcher;

partial class IndexSearcher
{
    public MemoizationMatchProvider<TInner> Memoize<TInner>(TInner inner)
        where TInner : IQueryMatch
    {
        return new MemoizationMatchProvider<TInner>(this, inner);
    }
}
