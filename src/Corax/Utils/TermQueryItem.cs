using System;

namespace Corax.Utils;

public readonly struct TermQueryItem
{
    public readonly Memory<byte> Item;
    public readonly long Density;

    public TermQueryItem(ReadOnlySpan<byte> item, long density)
    {
        Item = new Memory<byte>(item.ToArray());
        Density = density;
    }
}
