using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Utils;

namespace Corax.Querying;

public unsafe partial class IndexSearcher
{
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMatch OrderBy<TInner>(in TInner set, OrderMetadata metadata, NullsSortMode defaultNullsSortMode, int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        return new SortingMatch<TInner>(this, set, metadata, token, defaultNullsSortMode, take);
    }
    
        
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMultiMatch OrderBy<TInner>(in TInner set, OrderMetadata[] metadata, NullsSortMode defaultNullsSortMode,
        int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        return new SortingMultiMatch<TInner>(this, set, metadata, defaultNullsSortMode, take, token: token);
    }
}
