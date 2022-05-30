using System;
using System.Runtime.CompilerServices;
using Corax.Queries;
using Corax.Utils;
using Spatial4n.Shapes;

namespace Corax;

public unsafe partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMatch OrderByScore<TInner>(in TInner set, int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return SortingMatch.Create(new SortingMatch<TInner, BoostingComparer>(this, set, default(BoostingComparer), take));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMatch OrderByAscending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
        int take = Constants.IndexSearcher.TakeAll, object additionalData = null)
        where TInner : IQueryMatch
    {
        if (entryFieldType is MatchCompareFieldType.Alphanumeric)
            return OrderBy<TInner, SortingMatch.AlphanumericAscendingMatchComparer>(in set, fieldId,  MatchCompareFieldType.Sequence, take);
        
        return OrderBy<TInner, SortingMatch.AscendingMatchComparer>(in set, fieldId,   entryFieldType, take);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SortingMatch OrderByDescending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
        int take = Constants.IndexSearcher.TakeAll, object additionalData = null)
        where TInner : IQueryMatch
    {
        if (entryFieldType is MatchCompareFieldType.Alphanumeric)
            return OrderBy<TInner, SortingMatch.AlphanumericDescendingMatchComparer>(in set, fieldId, MatchCompareFieldType.Sequence, take);
        
        return OrderBy<TInner, SortingMatch.DescendingMatchComparer>(in set, fieldId, entryFieldType, take);
    }

    public SortingMatch OrderByDistance<TInner>(in TInner set, in OrderMetadata metadata,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        if (metadata.Ascending)
        {
            return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.SpatialAscendingMatchComparer>(this, set,
                    new SortingMatch.SpatialAscendingMatchComparer(this, in metadata), take));
        }
        
        return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.SpatialDescendingMatchComparer>(this, set,
                    new SortingMatch.SpatialDescendingMatchComparer(this, in metadata), take));
    }

    private SortingMatch OrderBy<TInner, TComparer>(in TInner set, int fieldId,  MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
        where TComparer : IMatchComparer
    {
        if (typeof(TComparer) == typeof(SortingMatch.AscendingMatchComparer))
        {
            return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.AscendingMatchComparer>(this, set, new SortingMatch.AscendingMatchComparer(this, fieldId, entryFieldType), take));
        }

        if (typeof(TComparer) == typeof(SortingMatch.DescendingMatchComparer))
        {
            return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.DescendingMatchComparer>(this, set,
                new SortingMatch.DescendingMatchComparer(this, fieldId, entryFieldType), take));
        }

        if (typeof(TComparer) == typeof(BoostingComparer))
        {
            return SortingMatch.Create(new SortingMatch<TInner, BoostingComparer>(this, set, default(BoostingComparer), take));
        }

        if (typeof(TComparer) == typeof(SortingMatch.AlphanumericAscendingMatchComparer))
        {
            return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.AlphanumericAscendingMatchComparer>(this, set, new SortingMatch.AlphanumericAscendingMatchComparer(this, fieldId, entryFieldType), take));
        }

        if (typeof(TComparer) == typeof(SortingMatch.AlphanumericDescendingMatchComparer))
        {
            return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.AlphanumericDescendingMatchComparer>(this, set, new SortingMatch.AlphanumericDescendingMatchComparer(this, fieldId, entryFieldType), take));
        }
        
        if (typeof(TComparer) == typeof(SortingMatch.CustomMatchComparer))
        {
            throw new ArgumentException($"Custom comparers can only be created through the {nameof(OrderByCustomOrder)}");
        }

        throw new ArgumentException($"The comparer of type {typeof(TComparer).Name} is not supported. Isn't {nameof(OrderByCustomOrder)} the right call for it?");
    }
    
    public SortingMatch OrderByCustomOrder<TInner>(in TInner set, int fieldId,
        delegate*<IndexSearcher, int, long, long, int> compareByIdFunc,
        delegate*<long, long, int> compareLongFunc,
        delegate*<double, double, int> compareDoubleFunc,
        delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> compareSequenceFunc,
        MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        // Federico: I don't even really know if we are going to find a use case for this. However, it was built for the purpose
        //           of showing that it is possible to build any custom group of functions. Why would we want to do this instead
        //           of just building a TComparer, I dont know. But for now the `CustomMatchComparer` can be built like this from
        //           static functions. 
        return SortingMatch.Create(new SortingMatch<TInner, SortingMatch.CustomMatchComparer>(
            this, set,
            new SortingMatch.CustomMatchComparer(
                this, fieldId,
                compareByIdFunc,
                compareLongFunc,
                compareDoubleFunc,
                compareSequenceFunc,
                entryFieldType
            ), 
            take));
    }
}
