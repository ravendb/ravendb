using Voron.Data.CompactTrees;

namespace Corax.Utils;

public readonly struct TermQueryItem
{
    public readonly CompactKey Item;
    public readonly long Density;

    public TermQueryItem(CompactKey item, long density)
    {
        Item = item;
        Density = density;
    }
}
