using System;
using System.Runtime.CompilerServices;

namespace Voron.Data.CompactTrees;

public readonly struct CompactKeyCacheScope : IDisposable
{
    public readonly CompactKey Key;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CompactKeyCacheScope(CompactTree tree)
    {
        Key = tree.AcquireKey();
    }

    public CompactKeyCacheScope(CompactTree tree, ReadOnlySpan<byte> key)
    {
        Key = tree.AcquireKey();
        Key.Set(key);
    }

    public CompactKeyCacheScope(CompactTree tree, CompactKey key)
    {
        Key = tree.AcquireKey();
        Key.Set(key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        Key?.Owner.ReleaseKey(Key);
    }
}
