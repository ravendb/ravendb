using System;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

public struct CompactKeyCacheScope : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private CompactKey _key;
    public CompactKey Key => _key;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CompactKeyCacheScope(LowLevelTransaction tx)
    {
        _llt = tx;
        _key = tx.AcquireCompactKey();
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, ReadOnlySpan<byte> key, long dictionaryId)
    {
        _llt = tx;
        _key = tx.AcquireCompactKey();
        _key.Set(key);
        _key.ChangeDictionary(dictionaryId);
    }

    public void Dispose()
    {
        // We are getting an unsafe references on an scoped object because we are disposing anyways. 
        _llt.ReleaseCompactKey(ref _key);
    }
}
