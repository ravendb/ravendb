using Corax.Queries;
using Sparrow.Server;

namespace Corax;

partial class IndexSearcher
{
    public MemoizationMatchProvider<TInner> Memoize<TInner>(TInner inner)
        where TInner : IQueryMatch
    {
        return new MemoizationMatchProvider<TInner>(Allocator, inner);
    }
}
