using System;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

public readonly struct CompactKeyCacheScope : IDisposable
{
    public readonly CompactKey Key;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CompactKeyCacheScope(LowLevelTransaction tx)
    {
        Key = tx.AcquireCompactKey();
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, ReadOnlySpan<byte> key)
    {
        Key = tx.AcquireCompactKey();
        Key.Set(key);
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, CompactKey key)
    {
        Key = tx.AcquireCompactKey();
        Key.Set(key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        Key?.Owner.ReleaseCompactKey(Key);
    }
}
