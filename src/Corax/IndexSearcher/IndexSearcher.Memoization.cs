using Corax.Queries;

namespace Corax;

partial class IndexSearcher
{
    public MemoizationMatchProvider<TInner> Memoize<TInner>(TInner inner)
        where TInner : IQueryMatch
    {
        return new MemoizationMatchProvider<TInner>(inner);
    }
}
