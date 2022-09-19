using System.Runtime.CompilerServices;
using Corax.Queries;

namespace Corax.Utils;

public sealed class MemoizationMatchProviderRef<TInner> : IMemoizationMatchSource where TInner : IQueryMatch
{
    private MemoizationMatchProvider<TInner> _value;

    public MemoizationMatchProviderRef(MemoizationMatchProvider<TInner> match)
    {
        _value = match;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MemoizationMatch Replay()
    {
        return _value.Replay();
    }
    
    public void Dispose()
    {
        _value.Dispose();
    }
}
