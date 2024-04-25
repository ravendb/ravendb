using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public MultiUnaryMatch CreateMultiUnaryMatch<TInner>(TInner inner, MultiUnaryItem[] unaryItems, CancellationToken token = default)
    where TInner : IQueryMatch
    {
        return MultiUnaryMatch.Create(new MultiUnaryMatch<TInner>(this, inner, unaryItems), token);
    }
}
