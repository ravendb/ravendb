using System;
using System.Runtime.CompilerServices;
using Corax.Queries;
using Corax.Queries.SortingMatches;
using Corax.Utils;

namespace Corax;

public unsafe partial class IndexSearcher
{
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMatch OrderBy<TInner>(in TInner set, OrderMetadata metadata,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return SortingMatch.Create(new SortingMatch<TInner>(this,  set, metadata, take));
    }
}
