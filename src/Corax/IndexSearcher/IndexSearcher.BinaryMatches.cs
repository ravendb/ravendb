using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Queries;
using Sparrow.Server;

namespace Corax;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BinaryMatch And<TInner, TOuter>(in TInner set1, in TOuter set2, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        // TODO: We need to create this code using a template or using typed delegates (which either way would need templating for boilerplate code generation)
        // If any of the generic types is not known to be a struct (calling from interface) the code executed will
        // do all the work to figure out what to emit. The cost is in instantiation not on execution.                         
        if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
        {
            return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldAnd(this, (TermMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
        {
            return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldAnd(this, (BinaryMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldAnd(this, (TermMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldAnd(this, (BinaryMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }

        return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldAnd(this, in set1, in set2, token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BinaryMatch Or<TInner, TOuter>(in TInner set1, in TOuter set2, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        // When faced with a MultiTermMatch and something else, lets first calculate the something else.
        if (set2.GetType() == typeof(MultiTermMatch) && set1.GetType() != typeof(MultiTermMatch))
            return Or( set2, set1);

        // If any of the generic types is not known to be a struct (calling from interface) the code executed will
        // do all the work to figure out what to emit. The cost is in instantiation not on execution. 
        if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
        {
            return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldOr(this, (TermMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
        {
            return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldOr(this, (BinaryMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldOr(this, (TermMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldOr(this, (BinaryMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }

        return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldOr(this, in set1, in set2, token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch AndNot<TInner, TOuter>(in TInner set1, in TOuter set2, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {

        // If any of the generic types is not known to be a struct (calling from interface) the code executed will
        // do all the work to figure out what to emit. The cost is in instantiation not on execution.                         
        if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
        {
            return AndNotMatch.Create(AndNotMatch<TermMatch, TermMatch>.Create(this, (TermMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
        {
            return AndNotMatch.Create(AndNotMatch<BinaryMatch, TermMatch>.Create(this, (BinaryMatch)(object)set1, (TermMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return AndNotMatch.Create(AndNotMatch<TermMatch, BinaryMatch>.Create(this, (TermMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }
        else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
        {
            return AndNotMatch.Create(AndNotMatch<BinaryMatch, BinaryMatch>.Create(this, (BinaryMatch)(object)set1, (BinaryMatch)(object)set2, token));
        }

        return AndNotMatch.Create(AndNotMatch<TInner, TOuter>.Create(this, in set1, in set2, token));
    }
}
