using System;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

public struct CompactKeyCacheScope : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private CompactKey _key;
    public readonly CompactKey Key => _key ?? throw new ObjectDisposedException(nameof(CompactKeyCacheScope));

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
        _llt.ReleaseCompactKey(ref _key);
    }
}
