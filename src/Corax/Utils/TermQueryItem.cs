using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Utils;

public readonly record struct TermQueryItem(CompactKey Item, long Density, in Slice Term);
