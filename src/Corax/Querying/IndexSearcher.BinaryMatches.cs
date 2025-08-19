using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BinaryMatch And<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        if (typeof(TInner) != typeof(IQueryMatch) && typeof(TOuter) != typeof(IQueryMatch))
            return Build(innerSet, outerSet, token);
        
        return MatchTypeAndBuild(innerSet, outerSet, token);
        BinaryMatch MatchTypeAndBuild<TFirstInner, TFirstOuter>(in TFirstInner firstInnerSet, in TFirstOuter firstOuterSet, in CancellationToken token)
            where TFirstInner : IQueryMatch
            where TFirstOuter : IQueryMatch 
            => QueryBuilderHelper.GetQueryType(firstInnerSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => OuterGenericMatcher((TermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => OuterGenericMatcher((BinaryMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => OuterGenericMatcher((MultiTermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => OuterGenericMatcher((AndNotMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => OuterGenericMatcher((BoostingMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => OuterGenericMatcher((SpatialMatch<NoBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => OuterGenericMatcher((SpatialMatch<HasBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => OuterGenericMatcher((MultiUnaryMatch)(object)firstInnerSet, firstOuterSet, token),
                _ => OuterGenericMatcher(firstInnerSet, firstOuterSet, token),
            };

        BinaryMatch OuterGenericMatcher<TSecondInner, TSecondOuter>(in TSecondInner secondInnerSet, in TSecondOuter secondOuterSet, in CancellationToken token = default)
            where TSecondInner : IQueryMatch
            where TSecondOuter : IQueryMatch
            => QueryBuilderHelper.GetQueryType(secondOuterSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => Build(secondInnerSet, (TermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => Build(secondInnerSet, (BinaryMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => Build(secondInnerSet, (MultiTermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => Build(secondInnerSet, (AndNotMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => Build(secondInnerSet, (BoostingMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => Build(secondInnerSet, (SpatialMatch<NoBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => Build(secondInnerSet, (SpatialMatch<HasBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => Build(secondInnerSet, (MultiUnaryMatch)(object)secondOuterSet, token),
                _ => Build(secondInnerSet, secondOuterSet, token),
            };

        BinaryMatch Build<TBuildInner, TBuildOuter>(in TBuildInner buildInnerSet, in TBuildOuter buildOuterSet, in CancellationToken token = default)
            where TBuildInner : IQueryMatch
            where TBuildOuter : IQueryMatch 
            => BinaryMatch.Create(BinaryMatch<TBuildInner, TBuildOuter, BinaryMatch.And>.YieldAnd(this, in buildInnerSet, in buildOuterSet, token));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BinaryMatch Or<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        // When faced with a MultiTermMatch and something else, lets first calculate the something else.
        if (outerSet is MultiTermMatch && innerSet.GetType() != typeof(MultiTermMatch))
            return Or(outerSet, innerSet);

        if (typeof(TInner) != typeof(IQueryMatch) && typeof(TOuter) != typeof(IQueryMatch))
            return Build(innerSet, outerSet, token);
        
        return MatchTypeAndBuild(innerSet, outerSet, token);
        
        BinaryMatch MatchTypeAndBuild<TFirstInner, TFirstOuter>(in TFirstInner firstInnerSet, in TFirstOuter firstOuterSet, in CancellationToken token)
            where TFirstInner : IQueryMatch
            where TFirstOuter : IQueryMatch 
            => QueryBuilderHelper.GetQueryType(firstInnerSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => OuterGenericMatcher((TermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => OuterGenericMatcher((BinaryMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => OuterGenericMatcher((MultiTermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => OuterGenericMatcher((AndNotMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => OuterGenericMatcher((BoostingMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => OuterGenericMatcher((SpatialMatch<NoBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => OuterGenericMatcher((SpatialMatch<HasBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => OuterGenericMatcher((MultiUnaryMatch)(object)firstInnerSet, firstOuterSet, token),
                _ => OuterGenericMatcher(firstInnerSet, firstOuterSet, token),
            };

        BinaryMatch OuterGenericMatcher<TSecondInner, TSecondOuter>(in TSecondInner secondInnerSet, in TSecondOuter secondOuterSet, in CancellationToken token = default)
            where TSecondInner : IQueryMatch
            where TSecondOuter : IQueryMatch 
            => QueryBuilderHelper.GetQueryType(secondOuterSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => Build(secondInnerSet, (TermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => Build(secondInnerSet, (BinaryMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => Build(secondInnerSet, (MultiTermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => Build(secondInnerSet, (AndNotMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => Build(secondInnerSet, (BoostingMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => Build(secondInnerSet, (SpatialMatch<NoBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => Build(secondInnerSet, (SpatialMatch<HasBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => Build(secondInnerSet, (MultiUnaryMatch)(object)secondOuterSet, token),
                _ => Build(secondInnerSet, secondOuterSet, token),
            };

        BinaryMatch Build<TBuildInner, TBuildOuter>(in TBuildInner buildInnerSet, in TBuildOuter buildOuterSet, in CancellationToken token = default)
            where TBuildInner : IQueryMatch
            where TBuildOuter : IQueryMatch 
            => BinaryMatch.Create(BinaryMatch<TBuildInner, TBuildOuter, BinaryMatch.Or>.YieldOr(this, in buildInnerSet, in buildOuterSet, token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch AndNot<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        if (typeof(TInner) != typeof(IQueryMatch) && typeof(TOuter) != typeof(IQueryMatch))
            return Build(innerSet, outerSet, token);

        return MatchTypeAndBuild(innerSet, outerSet, token);
        
        AndNotMatch MatchTypeAndBuild<TFirstInner, TFirstOuter>(in TFirstInner firstInnerSet, in TFirstOuter firstOuterSet, in CancellationToken token)
            where TFirstInner : IQueryMatch
            where TFirstOuter : IQueryMatch 
            => QueryBuilderHelper.GetQueryType(firstInnerSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => OuterGenericMatcher((TermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => OuterGenericMatcher((BinaryMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => OuterGenericMatcher((MultiTermMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => OuterGenericMatcher((AndNotMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => OuterGenericMatcher((BoostingMatch)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => OuterGenericMatcher((SpatialMatch<NoBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => OuterGenericMatcher((SpatialMatch<HasBoosting>)(object)firstInnerSet, firstOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => OuterGenericMatcher((MultiUnaryMatch)(object)firstInnerSet, firstOuterSet, token),
                _ => OuterGenericMatcher(firstInnerSet, firstOuterSet, token),
            };

        AndNotMatch OuterGenericMatcher<TSecondInner, TSecondOuter>(in TSecondInner secondInnerSet, in TSecondOuter secondOuterSet, in CancellationToken token = default)
            where TSecondInner : IQueryMatch
            where TSecondOuter : IQueryMatch 
            => QueryBuilderHelper.GetQueryType(secondOuterSet) switch
            {
                QueryBuilderHelper.QueryType.TermMatch => Build(secondInnerSet, (TermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BinaryMatch => Build(secondInnerSet, (BinaryMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiTermMatch => Build(secondInnerSet, (MultiTermMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.AndNotMatch => Build(secondInnerSet, (AndNotMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.BoostingMatch => Build(secondInnerSet, (BoostingMatch)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchNoBoosting => Build(secondInnerSet, (SpatialMatch<NoBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.SpatialMatchHasBoosting => Build(secondInnerSet, (SpatialMatch<HasBoosting>)(object)secondOuterSet, token),
                QueryBuilderHelper.QueryType.MultiUnaryMatch => Build(secondInnerSet, (MultiUnaryMatch)(object)secondOuterSet, token),
                _ => Build(secondInnerSet, secondOuterSet, token),
            };

        AndNotMatch Build<TBuildInner, TBuildOuter>(in TBuildInner buildInnerSet, in TBuildOuter buildOuterSet, in CancellationToken token = default)
            where TBuildInner : IQueryMatch
            where TBuildOuter : IQueryMatch 
            => AndNotMatch.Create(AndNotMatch<TBuildInner, TBuildOuter>.Create(this, in buildInnerSet, in buildOuterSet, token));
    }
}
