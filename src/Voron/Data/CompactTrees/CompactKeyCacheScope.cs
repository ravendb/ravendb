using System;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

public readonly struct CompactKeyCacheScope : IDisposable
{
    private readonly LowLevelTransaction _llt;
    public readonly CompactKey Key;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CompactKeyCacheScope(LowLevelTransaction tx)
    {
        _llt = tx;
        Key = tx.AcquireCompactKey();
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, ReadOnlySpan<byte> key, long dictionaryId)
    {
        _llt = tx;
        Key = tx.AcquireCompactKey();
        Key.Set(key);
        Key.ChangeDictionary(dictionaryId);
    }

    public void Dispose()
    {
        // We are getting an unsafe references on an scoped object because we are disposing anyways. 
        _llt.ReleaseCompactKey(ref Unsafe.AsRef(Key));
    }
}
