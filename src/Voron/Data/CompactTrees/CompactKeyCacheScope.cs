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

    public CompactKeyCacheScope(LowLevelTransaction tx, ReadOnlySpan<byte> key, long dictionaryId)
    {
        Key = tx.AcquireCompactKey();
        Key.Set(key);
        Key.ChangeDictionary(dictionaryId);
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, CompactKey key)
    {
        Key = tx.AcquireCompactKey();
        Key.Set(key);
    }

    public CompactKeyCacheScope(LowLevelTransaction tx, int keyLengthInBits, ReadOnlySpan<byte> encodedKey, long dictionaryId)
    {
        Key = tx.AcquireCompactKey();
        Key.Set(keyLengthInBits, encodedKey, dictionaryId);
    }

    public void Dispose()
    {
        Key?.Dispose();
    }
}
